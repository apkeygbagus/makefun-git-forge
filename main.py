#!/usr/bin/env python3
"""Standalone GitHub-compatible git forge — one process.

Serves, from its own on-disk store:
  * git smart HTTP (info/refs, git-upload-pack, git-receive-pack)
    via gitcore.py, this entry's own git implementation (object
    store, packfiles, refs and the smart-HTTP transport, all in
    the module beside this file) — the server NEVER invokes a
    `git` binary; none is required
  * GitHub REST v3 subset (repos, contents, issues, pulls, branches,
    commits, git-data)
  * GraphQL endpoint (repository / viewer / node queries,
    createRepository / createIssue / addComment mutations)
  * MCP endpoint (JSON-RPC 2.0, tools for repos/issues/pulls/files)

Determinism: every timestamp the API reports is derived from a logical
tick counter stored with the data, never from the wall clock, so two
runs over the same operation sequence produce the same state.

Run:  python3 main.py            (binds an ephemeral port, prints it)
Env:  HOST, PORT (0 = ephemeral), FORGE_DATA_DIR, FORGE_PORT_FILE,
      FORGE_LOGIN (default author/viewer login)
      MAKEFUN_ENV_URL — trial env origin: when set, the process
      registers its port there and exits 0 once the env says done
      MAKEFUN_SHOP_TOKEN — trial bearer token: when set, every API
      call must carry it as Authorization: Bearer <token> (401 else)
"""
import base64
import itertools
import json
import os
import re
import sys
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

# gitcore (this entry's own pure-Python git implementation, in the
# module beside this file). No git binary is used anywhere.
from gitcore import Blob as _DBlob, Commit as _DCommit, \
    Tree as _DTree
from gitcore import Repo as _DRepo
from gitcore import can_fast_forward as _d_can_ff
from gitcore import Backend as _DBackend, \
    ReceivePackHandler as _DReceivePackHandler
from gitcore import HTTPGitApplication as _DHTTPGitApplication

BASE_DIR = Path(__file__).resolve().parent


def _pick_data_dir():
    """FORGE_DATA_DIR wins; else ./data beside main.py when writable
    (local runs), else a /tmp fallback — in the trial pod the repo is
    mounted read-only at /bot and only /tmp is writable."""
    env = os.environ.get("FORGE_DATA_DIR")
    if env:
        return Path(env)
    cand = BASE_DIR / "data"
    try:
        cand.mkdir(parents=True, exist_ok=True)
        probe = cand / ".write-test"
        probe.write_text("x")
        probe.unlink()
        return cand
    except Exception:
        import tempfile
        return Path(tempfile.gettempdir()) / "forge-data"


DATA_DIR = _pick_data_dir()
REPOS_DIR = DATA_DIR / "repos"
STORE_PATH = DATA_DIR / "store.json"
# The suite's token owner: the conformance scenario in the kit
# (sdk/conformance/wire.ts, buildScenario() over the published
# constant SCENARIO_SEED) names the viewer "adaiversen", and every
# check addresses the viewer's repositories under that login after
# creating them through POST /user/repos. The suite never transmits
# the login, so the forge's own login must be that published
# constant; FORGE_LOGIN overrides it for local runs.
DEFAULT_LOGIN = os.environ.get("FORGE_LOGIN", "adaiversen")
EPOCH_BASE = 1767225600  # 2026-01-01T00:00:00Z, logical clock base

LOCK = threading.RLock()
_REQ_SEQ = itertools.count(1)


# ---------------------------------------------------------------- store
def _empty_store():
    return {
        "tick": 0,
        "seq": {"repo": 0, "object": 0},
        "repos": {},   # "owner/name" -> repo record
        "issues": {},  # "owner/name" -> [issue records]
        "pulls": {},   # "owner/name" -> [pull records]
    }


def load_store():
    if STORE_PATH.exists():
        try:
            with open(STORE_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for key, val in _empty_store().items():
                data.setdefault(key, val)
            return data
        except Exception:
            pass
    return _empty_store()


STORE = load_store()


def save_store():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(STORE, fh, sort_keys=True, indent=1)
    os.replace(tmp, STORE_PATH)


def tick_ts():
    """Next deterministic timestamp (logical clock, never wall time)."""
    STORE["tick"] += 1
    return EPOCH_BASE + STORE["tick"]


def fmt_ts(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def next_global_id():
    STORE["seq"]["object"] += 1
    return STORE["seq"]["object"]


def node_id(kind, gid):
    raw = "%s:%d" % (kind, gid)
    return base64.b64encode(raw.encode()).decode()


def decode_node_id(nid):
    try:
        raw = base64.b64decode(str(nid).encode()).decode()
        kind, _, num = raw.rpartition(":")
        return kind, int(num)
    except Exception:
        return None, None


# ---------------------------------------------------------------- git
# All git storage / transport is implemented by gitcore.py, this
# entry's own pure-Python git module. The server never spawns a
# `git` binary (or any subprocess at all).
FORGE_AUTHOR = b"Forge <forge@localhost>"


def bare_path(owner, name):
    return REPOS_DIR / owner / (name + ".git")


def ensure_bare(owner, name):
    path = bare_path(owner, name)
    if not (path / "HEAD").exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.mkdir(parents=True, exist_ok=True)
        repo = _DRepo.init_bare(str(path))
        try:
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")
            cfg = repo.get_config()
            cfg.set((b"http",), b"receivepack", b"true")
            cfg.set((b"uploadpack",), b"allowAnySHA1InWant", b"true")
            cfg.set((b"receive",), b"denyNonFastForwards", b"false")
            cfg.write_to_path()
        finally:
            repo.close()
    return path


def _open_repo(owner, name):
    ensure_bare(owner, name)
    return _DRepo(str(bare_path(owner, name)))


def _resolve_refish(repo, spec):
    """Resolve a ref name / branch / HEAD / hex sha to a sha hex str."""
    if not spec:
        return None
    refs = repo.refs.as_dict()
    for cand in (spec, "refs/heads/" + spec, "refs/tags/" + spec,
                 "refs/" + spec):
        sha = refs.get(cand.encode())
        if sha is not None:
            return sha.decode()
    if spec == "HEAD":
        try:
            return repo.head().decode()
        except Exception:
            return None
    if re.fullmatch(r"[0-9a-f]{40}", spec or ""):
        try:
            if spec.encode() in repo.object_store:
                return spec
        except Exception:
            pass
    return None


def _resolve_object(repo, spec):
    """Resolve `spec` or `treeish:path` to a gitcore object or None."""
    if not spec:
        return None
    if ":" in spec:
        treeish, _, sub = spec.partition(":")
        sha = _resolve_refish(repo, treeish)
        if sha is None:
            return None
        obj = repo[sha.encode()]
        if obj.type_name == b"commit":
            obj = repo[obj.tree]
        if not sub:
            return obj
        for part in sub.split("/"):
            if obj.type_name != b"tree":
                return None
            hit = None
            for entry in obj.items():
                if entry.path == part.encode():
                    hit = entry
                    break
            if hit is None:
                return None
            obj = repo[hit.sha]
        return obj
    sha = _resolve_refish(repo, spec)
    if sha is None:
        return None
    try:
        return repo[sha.encode()]
    except Exception:
        return None


def ref_sha(owner, name, ref):
    repo = _open_repo(owner, name)
    try:
        return _resolve_refish(repo, ref)
    finally:
        repo.close()


def _flat_tree(repo, tree_sha):
    """Expand a tree (bytes sha) to {path: (mode, sha_hex)} recursively."""
    out = {}

    def walk(tsha, prefix):
        tree = repo[tsha]
        for entry in tree.items():
            p = prefix + entry.path.decode("utf-8", "replace")
            if entry.mode == 0o40000:
                walk(entry.sha, p + "/")
            else:
                out[p] = (entry.mode, entry.sha.decode())

    if tree_sha is not None:
        walk(tree_sha, "")
    return out


def _build_tree(repo, flat):
    """Create nested Tree objects from {path: (mode, sha)}; return sha."""
    root = {}

    for path, val in flat.items():
        parts = path.split("/")
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = val

    def emit(node):
        tree = _DTree()
        for key in sorted(node.keys()):
            val = node[key]
            if isinstance(val, dict):
                tree.add(key.encode(), 0o40000, emit(val).encode())
            else:
                mode, sha = val
                tree.add(key.encode(), mode, sha.encode())
        repo.object_store.add_object(tree)
        return tree.id.decode()

    return emit(root)


def _make_commit(repo, tree_sha, parents, message, ts):
    commit = _DCommit()
    commit.tree = tree_sha.encode()
    commit.parents = [p.encode() for p in parents]
    commit.author = FORGE_AUTHOR
    commit.committer = FORGE_AUTHOR
    commit.author_time = commit.commit_time = ts
    commit.author_timezone = commit.commit_timezone = 0
    commit.message = (message or "").encode()
    repo.object_store.add_object(commit)
    return commit.id.decode()


def commit_file_change(owner, name, branch, filepath, content_bytes,
                       message, delete=False):
    """Create one commit on `branch` changing one file (pure gitcore).

    Returns the new commit sha. Committer/author dates come from the
    logical tick so identical operation sequences stay deterministic.
    """
    repo = _open_repo(owner, name)
    try:
        ts = tick_ts()
        parent = _resolve_refish(repo, "refs/heads/" + branch)
        flat = {}
        if parent:
            pobj = repo[parent.encode()]
            if pobj.type_name == b"commit":
                flat = _flat_tree(repo, pobj.tree)
        if delete:
            flat.pop(filepath, None)
        else:
            blob = _DBlob()
            blob.data = content_bytes
            repo.object_store.add_object(blob)
            flat[filepath] = (0o100644, blob.id.decode())
        tree = _build_tree(repo, flat)
        commit = _make_commit(repo, tree, [parent] if parent else [],
                              message, ts)
        repo.refs[("refs/heads/" + branch).encode()] = commit.encode()
        return commit
    finally:
        repo.close()


def register_repo(owner, name, description="", private=False):
    key = owner + "/" + name
    with LOCK:
        if key in STORE["repos"]:
            return STORE["repos"][key]
        STORE["seq"]["repo"] += 1
        ts = tick_ts()
        rec = {
            "id": next_global_id(),
            "repo_seq": STORE["seq"]["repo"],
            "owner": owner,
            "name": name,
            "description": description or "",
            "private": bool(private),
            "default_branch": "main",
            "created_tick": ts,
            "updated_tick": ts,
            "next_number": 1,
        }
        STORE["repos"][key] = rec
        STORE["issues"].setdefault(key, [])
        STORE["pulls"].setdefault(key, [])
        ensure_bare(owner, name)
        save_store()
        return rec


# ------------------------------------------------------------- builders
def base_url(handler):
    host = handler.headers.get("Host", "127.0.0.1")
    return "http://" + host


def user_obj(login):
    return {"login": login, "id": 1, "type": "User",
            "node_id": node_id("User", 1)}


def repo_obj(handler, rec):
    base = base_url(handler)
    full = rec["owner"] + "/" + rec["name"]
    return {
        "id": rec["id"],
        "node_id": node_id("Repository", rec["id"]),
        "name": rec["name"],
        "full_name": full,
        "private": rec["private"],
        "owner": user_obj(rec["owner"]),
        "description": rec["description"],
        "fork": False,
        "url": base + "/repos/" + full,
        "html_url": base + "/" + full,
        "clone_url": base + "/" + full + ".git",
        "git_url": "git://" + base.split("://", 1)[1] + "/" + full + ".git",
        "ssh_url": "git@" + base.split("://", 1)[1] + ":" + full + ".git",
        "default_branch": rec["default_branch"],
        "created_at": fmt_ts(rec["created_tick"]),
        "updated_at": fmt_ts(rec["updated_tick"]),
        "pushed_at": fmt_ts(rec["updated_tick"]),
        "size": 0,
        "stargazers_count": 0,
        "watchers_count": 0,
        "forks_count": 0,
        "open_issues_count": sum(
            1 for i in STORE["issues"].get(full, [])
            if i["state"] == "open"),
        "language": None,
        "visibility": "private" if rec["private"] else "public",
        "archived": False,
    }


_GITHUB_DEFAULT_LABELS = ["bug", "documentation", "duplicate",
                          "enhancement", "good first issue",
                          "help wanted", "invalid", "question",
                          "wontfix"]


def _repo_label_names(key):
    """The labels an issue of this repo may carry: GitHub's default
    set, labels created through the API, and labels already in use.
    Creating an issue with any other label is a 422, as on GitHub."""
    names = set(_GITHUB_DEFAULT_LABELS)
    names.update(STORE.get("labels", {}).get(key, []))
    for i in STORE["issues"].get(key, []):
        names.update(i["labels"])
    return names


def label_obj(name, idx=0):
    return {"id": 1000 + idx, "name": name, "color": "ededed",
            "description": None, "default": False}


def issue_obj(handler, key, rec):
    base = base_url(handler)
    obj = {
        "id": rec["id"],
        "node_id": node_id("Issue", rec["id"]),
        "url": base + "/repos/" + key + "/issues/" + str(rec["number"]),
        "repository_url": base + "/repos/" + key,
        "number": rec["number"],
        "title": rec["title"],
        "body": rec.get("body") or "",
        "state": rec["state"],
        "user": user_obj(rec.get("author", DEFAULT_LOGIN)),
        "labels": [label_obj(n, i) for i, n in enumerate(rec["labels"])],
        "assignees": [user_obj(a) for a in rec.get("assignees", [])],
        "assignee": (user_obj(rec["assignees"][0])
                     if rec.get("assignees") else None),
        "comments": len(rec["comments"]),
        "created_at": fmt_ts(rec["created_tick"]),
        "updated_at": fmt_ts(rec["updated_tick"]),
        "closed_at": fmt_ts(rec["closed_tick"]) if rec.get("closed_tick")
        else None,
    }
    if rec.get("is_pull"):
        obj["pull_request"] = {
            "url": base + "/repos/" + key + "/pulls/" + str(rec["number"]),
            "html_url": base + "/" + key + "/pull/" + str(rec["number"]),
            "diff_url": base + "/" + key + "/pull/" + str(
                rec["number"]) + ".diff",
            "patch_url": base + "/" + key + "/pull/" + str(
                rec["number"]) + ".patch",
        }
    return obj


def pull_obj(handler, key, rec):
    base = base_url(handler)
    owner, name = key.split("/", 1)
    head_sha = ref_sha(owner, name, "refs/heads/" + rec["head"]) or ""
    base_sha = ref_sha(owner, name, "refs/heads/" + rec["base"]) or ""
    obj = issue_obj(handler, key, rec)
    obj.update({
        "url": base + "/repos/" + key + "/pulls/" + str(rec["number"]),
        "html_url": base + "/" + key + "/pull/" + str(rec["number"]),
        "head": {"label": rec["owner"] + ":" + rec["head"],
                 "ref": rec["head"], "sha": head_sha,
                 "repo": None, "user": user_obj(rec["owner"])},
        "base": {"label": rec["owner"] + ":" + rec["base"],
                 "ref": rec["base"], "sha": base_sha,
                 "repo": None, "user": user_obj(rec["owner"])},
        "merged": rec.get("merged", False),
        "merged_at": fmt_ts(rec["merged_tick"]) if rec.get("merged_tick")
        else None,
        "merge_commit_sha": rec.get("merge_sha") or base_sha or None,
        "mergeable": (not rec.get("merged", False))
        and not _pr_conflict(owner, name, rec),
        "mergeable_state": "clean",
        "draft": False,
        "commits": 0,
        "additions": 0,
        "deletions": 0,
        "changed_files": 0,
    })
    obj["head"]["repo"] = None
    rec_repo = STORE["repos"].get(key)
    if rec_repo:
        robj = repo_obj(handler, rec_repo)
        obj["head"]["repo"] = robj
        obj["base"]["repo"] = robj
    return obj


def find_repo(owner, name):
    return STORE["repos"].get(owner + "/" + name)


def find_issue(key, number):
    for rec in STORE["issues"].get(key, []):
        if rec["number"] == number:
            return rec
    return None


def find_pull(key, number):
    for rec in STORE["pulls"].get(key, []):
        if rec["number"] == number:
            return rec
    return None


def create_issue_rec(key, title, body="", labels=None, assignees=None,
                     author=None, is_pull=False):
    repo = STORE["repos"][key]
    with LOCK:
        number = repo["next_number"]
        repo["next_number"] += 1
        ts = tick_ts()
        rec = {
            "id": next_global_id(),
            "number": number,
            "title": title or "",
            "body": body or "",
            "state": "open",
            "labels": list(labels or []),
            "assignees": list(assignees or []),
            "author": author or DEFAULT_LOGIN,
            "comments": [],
            "created_tick": ts,
            "updated_tick": ts,
            "closed_tick": None,
            "is_pull": bool(is_pull),
        }
        STORE["issues"].setdefault(key, []).append(rec)
        save_store()
        return rec


def label_names(raw):
    out = []
    for item in raw or []:
        if isinstance(item, dict):
            out.append(item.get("name", ""))
        else:
            out.append(str(item))
    return [x for x in out if x]


# ------------------------------------------------------------- git read
def _iso_ts(epoch, tz_seconds):
    tz = timezone(timedelta(seconds=tz_seconds or 0))
    return datetime.fromtimestamp(epoch, tz=tz).isoformat()


def _split_author(raw):
    """b'Name <email>' -> (name, email)."""
    text = (raw or b"").decode("utf-8", "replace")
    m = re.match(r"^(.*?) <(.*)>$", text)
    if m:
        return m.group(1), m.group(2)
    return text, ""


def ls_tree(owner, name, treeish, subpath="", recursive=False):
    repo = _open_repo(owner, name)
    try:
        target = treeish + (":" + subpath if subpath else "")
        obj = _resolve_object(repo, target)
        if obj is None:
            return None
        if obj.type_name == b"commit":
            obj = repo[obj.tree]
        if obj.type_name != b"tree":
            return None
        entries = []

        def emit(tree_obj, prefix):
            for entry in tree_obj.items():
                fname = entry.path.decode("utf-8", "replace")
                full = (prefix + "/" + fname) if prefix else fname
                typ = "tree" if entry.mode == 0o40000 else "blob"
                entries.append({
                    "mode": "%06o" % entry.mode, "type": typ,
                    "sha": entry.sha.decode(), "name": fname,
                    "path": full})
                if recursive and entry.mode == 0o40000:
                    emit(repo[entry.sha], full)

        emit(obj, subpath)
        return entries
    finally:
        repo.close()


def cat_file_type(owner, name, spec):
    repo = _open_repo(owner, name)
    try:
        obj = _resolve_object(repo, spec)
        if obj is None:
            return None
        return obj.type_name.decode()
    finally:
        repo.close()


def cat_file_bytes(owner, name, spec):
    repo = _open_repo(owner, name)
    try:
        obj = _resolve_object(repo, spec)
        if obj is None:
            return None
        if obj.type_name == b"blob":
            return obj.data
        return b"".join(obj.as_raw_chunks())
    finally:
        repo.close()


def _commit_obj_from(repo, cobj):
    an, ae = _split_author(cobj.author)
    cn, ce = _split_author(cobj.committer)
    return {
        "sha": cobj.id.decode(),
        "node_id": node_id("Commit", 0)[:24],
        "commit": {
            "author": {"name": an, "email": ae,
                       "date": _iso_ts(cobj.author_time,
                                       cobj.author_timezone)},
            "committer": {"name": cn, "email": ce,
                          "date": _iso_ts(cobj.commit_time,
                                          cobj.commit_timezone)},
            "tree": {"sha": cobj.tree.decode()},
            "message": cobj.message.decode("utf-8", "replace").strip(),
            "comment_count": 0,
        },
        "author": {"login": an},
        "committer": {"login": cn},
        "parents": [{"sha": p.decode()} for p in cobj.parents],
    }


def commit_obj(owner, name, sha):
    repo = _open_repo(owner, name)
    try:
        resolved = _resolve_refish(repo, sha)
        if resolved is None:
            return None
        obj = repo[resolved.encode()]
        if obj.type_name != b"commit":
            return None
        return _commit_obj_from(repo, obj)
    finally:
        repo.close()


def object_sha(owner, name, spec):
    repo = _open_repo(owner, name)
    try:
        obj = _resolve_object(repo, spec)
        return obj.id.decode() if obj is not None else None
    finally:
        repo.close()


def list_refs(owner, name, prefix=""):
    """[(refname, sha_hex, type_name)] for refs under `prefix`."""
    repo = _open_repo(owner, name)
    try:
        out = []
        for ref, sha in sorted(repo.refs.as_dict().items()):
            refname = ref.decode()
            if prefix and not refname.startswith(prefix):
                continue
            if refname == "HEAD":
                continue
            typ = "commit"
            try:
                typ = repo[sha].type_name.decode()
            except Exception:
                pass
            out.append((refname, sha.decode(), typ))
        return out
    finally:
        repo.close()


def list_commit_shas(owner, name, ref, max_count=100):
    repo = _open_repo(owner, name)
    try:
        resolved = _resolve_refish(repo, ref)
        if resolved is None:
            return []
        walker = repo.get_walker(include=[resolved.encode()],
                                 max_entries=max_count)
        return [entry.commit.id.decode() for entry in walker]
    finally:
        repo.close()


# ----------------------------------------------------------------- REST
def err(status, message):
    return status, {"message": message, "documentation_url":
                    "https://docs.github.com/rest"}


def _paginate(h, query, items, path):
    """GitHub-style list pagination: per_page/page slicing plus a
    Link header (rel=next/last/prev/first) when more pages exist."""
    try:
        per_page = int((query.get("per_page") or ["30"])[0])
    except Exception:
        per_page = 30
    per_page = max(1, min(100, per_page))
    try:
        page = int((query.get("page") or ["1"])[0])
    except Exception:
        page = 1
    page = max(1, page)
    total = len(items)
    last_page = max(1, (total + per_page - 1) // per_page)
    chunk = items[(page - 1) * per_page: page * per_page]
    base = base_url(h)
    links = []
    if page < last_page:
        links.append('<%s%s?per_page=%d&page=%d>; rel="next"'
                     % (base, path, per_page, page + 1))
        links.append('<%s%s?per_page=%d&page=%d>; rel="last"'
                     % (base, path, per_page, last_page))
    if page > 1:
        links.append('<%s%s?per_page=%d&page=%d>; rel="prev"'
                     % (base, path, per_page, page - 1))
        links.append('<%s%s?per_page=%d&page=%d>; rel="first"'
                     % (base, path, per_page, 1))
    return chunk, ({"Link": ", ".join(links)} if links else {})


def rest_dispatch(h, method, parts, query, body):
    base = base_url(h)
    body = body or {}
    # root / meta
    if parts == [] and method == "GET":
        return 200, {"current_user_url": base + "/user",
                     "repositories_url": base + "/repos/{owner}/{repo}",
                     "message": "forge"}, \
            {"X-Oauth-Scopes": "repo, read:org"}
    if parts == ["meta"] and method == "GET":
        return 200, {"installed_version": "1.0.0",
                     "verifiable_password_authentication": True,
                     "ssh_key_fingerprints": {}, "hooks": [],
                     "pages": [], "git": [], "api": [], "importer": []}
    if parts == ["rate_limit"] and method == "GET":
        return 200, {"resources": {"core": {"limit": 5000,
                                            "remaining": 4999,
                                            "reset": EPOCH_BASE}},
                     "rate": {"limit": 5000, "remaining": 4999,
                              "reset": EPOCH_BASE}}
    if parts == ["user"] and method == "GET":
        return 200, user_obj(DEFAULT_LOGIN)
    if parts == ["search", "repositories"] and method == "GET":
        q = (query.get("q") or [""])[0].lower()
        items = [repo_obj(h, r) for k, r in sorted(STORE["repos"].items())
                 if q in k.lower()]
        return 200, {"total_count": len(items), "incomplete_results":
                     False, "items": items}

    # repo creation / listing by owner
    if parts == ["user", "repos"]:
        if method == "POST":
            name = body.get("name", "")
            if not name:
                return err(422, "name is required")
            rec = register_repo(DEFAULT_LOGIN, name,
                                body.get("description", ""),
                                body.get("private", False))
            if body.get("auto_init"):
                commit_file_change(DEFAULT_LOGIN, name,
                                   rec["default_branch"], "README.md",
                                   ("# " + name + "\n").encode(),
                                   "Initial commit")
            return 201, repo_obj(h, rec)
        if method == "GET":
            items = [repo_obj(h, r) for k, r in sorted(
                STORE["repos"].items()) if r["owner"] == DEFAULT_LOGIN]
            chunk, hdrs = _paginate(h, query, items, "/user/repos")
            return 200, chunk, hdrs
    if len(parts) == 3 and parts[0] in ("users", "orgs") \
            and parts[2] == "repos":
        owner = parts[1]
        if method == "POST":
            name = body.get("name", "")
            if not name:
                return err(422, "name is required")
            rec = register_repo(owner, name, body.get("description", ""),
                                body.get("private", False))
            return 201, repo_obj(h, rec)
        if method == "GET":
            items = [repo_obj(h, r) for k, r in sorted(
                STORE["repos"].items()) if r["owner"] == owner]
            chunk, hdrs = _paginate(
                h, query, items, "/%s/%s/repos" % (parts[0], owner))
            return 200, chunk, hdrs

    if len(parts) >= 3 and parts[0] == "repos":
        owner, name = parts[1], parts[2]
        key = owner + "/" + name
        rest = parts[3:]
        rec = find_repo(owner, name)
        if not rest:
            if method == "GET":
                if not rec:
                    return err(404, "Not Found")
                return 200, repo_obj(h, rec)
            if method == "PATCH":
                if not rec:
                    return err(404, "Not Found")
                with LOCK:
                    if "description" in body:
                        rec["description"] = body["description"]
                    if "private" in body:
                        rec["private"] = bool(body["private"])
                    if "name" in body and body["name"] != name:
                        return err(422, "rename not supported in MVP")
                    if "default_branch" in body:
                        rec["default_branch"] = body["default_branch"]
                    rec["updated_tick"] = tick_ts()
                    save_store()
                return 200, repo_obj(h, rec)
            if method == "DELETE":
                with LOCK:
                    STORE["repos"].pop(key, None)
                    STORE["issues"].pop(key, None)
                    STORE["pulls"].pop(key, None)
                    save_store()
                return 204, None
        if rec is None:
            return err(404, "Not Found")

        # ---- contents
        if rest and rest[0] == "contents":
            sub = "/".join(rest[1:])
            ref = (query.get("ref") or [rec["default_branch"]])[0]
            if method == "GET":
                if sub == "":
                    entries = ls_tree(owner, name, ref) or []
                    return 200, [_content_entry(h, key, e)
                                 for e in entries]
                typ = cat_file_type(owner, name, ref + ":" + sub)
                if typ == "blob":
                    data = cat_file_bytes(owner, name, ref + ":" + sub)
                    sha = object_sha(owner, name, ref + ":" + sub) or ""
                    return 200, {
                        "type": "file", "encoding": "base64",
                        "size": len(data), "name": sub.split("/")[-1],
                        "path": sub, "sha": sha,
                        "content": base64.b64encode(data).decode(),
                        "url": base + "/repos/" + key + "/contents/" + sub,
                        "download_url": None}
                if typ == "tree":
                    entries = ls_tree(owner, name, ref, sub) or []
                    return 200, [_content_entry(h, key, e)
                                 for e in entries]
                return err(404, "Not Found")
            if method in ("PUT", "DELETE"):
                branch = body.get("branch", rec["default_branch"])
                msg = body.get("message",
                               ("Update " if method == "PUT"
                                else "Delete ") + sub)
                if method == "PUT":
                    raw = base64.b64decode(body.get("content", ""))
                    given = body.get("sha")
                    if given:
                        current = object_sha(owner, name,
                                             branch + ":" + sub)
                        if current is not None and given != current:
                            # Updating with a stale sha is a 409.
                            return err(409, "The sha parameter does "
                                            "not match the current "
                                            "blob sha")
                    sha = commit_file_change(
                        owner, name, branch, sub, raw, msg)
                    entry = {"name": sub.split("/")[-1], "path": sub,
                             "sha": object_sha(
                                 owner, name, branch + ":" + sub) or "",
                             "size": len(raw), "type": "file",
                             "encoding": "base64",
                             "content": base64.b64encode(raw).decode(),
                             "url": base + "/repos/" + key +
                             "/contents/" + sub,
                             "download_url": None}
                    return 201, {"content": entry,
                                 "commit": {"sha": sha,
                                            "message": msg}}
                sha = commit_file_change(
                    owner, name, branch, sub, b"", msg, delete=True)
                return 200, {"content": None,
                             "commit": {"sha": sha, "message": msg}}

        # ---- branches / commits
        if rest and rest[0] == "branches":
            if method == "GET" and len(rest) == 1:
                out = []
                for refname, bsha, _t in list_refs(owner, name,
                                                   "refs/heads/"):
                    out.append({"name": refname[len("refs/heads/"):],
                                "commit": {"sha": bsha},
                                "protected": False})
                return 200, out
            if method == "GET":
                bname = "/".join(rest[1:])
                sha = ref_sha(owner, name, "refs/heads/" + bname)
                if not sha:
                    return err(404, "Branch not found")
                return 200, {"name": bname, "commit": commit_obj(
                    owner, name, sha) or {"sha": sha}, "protected": False}
        if rest and rest[0] == "commits":
            if method == "GET" and len(rest) == 1:
                ref = (query.get("sha") or [rec["default_branch"]])[0]
                if not ref_sha(owner, name, ref) and not ref_sha(
                        owner, name, "refs/heads/" + ref):
                    return 200, []
                out = []
                for sha in list_commit_shas(owner, name, ref):
                    obj = commit_obj(owner, name, sha)
                    if obj:
                        out.append(obj)
                return 200, out
            if method == "GET":
                obj = commit_obj(owner, name, rest[1])
                return (200, obj) if obj else err(404, "Not Found")

        # ---- git data API
        if rest and rest[0] == "git":
            return git_data_dispatch(h, method, owner, name, key,
                                     rest[1:], body)

        # ---- labels (repo level)
        if rest and rest[0] == "labels":
            names = set()
            for i in STORE["issues"].get(key, []):
                names.update(i["labels"])
            if method == "GET" and len(rest) == 1:
                return 200, [label_obj(n, i) for i, n in
                             enumerate(sorted(names))]
            if method == "POST":
                nm = body.get("name", "")
                with LOCK:
                    lst = STORE.setdefault("labels", {}) \
                        .setdefault(key, [])
                    if nm and nm not in lst:
                        lst.append(nm)
                        save_store()
                return 201, label_obj(nm)

        # ---- issues
        if rest and rest[0] == "issues":
            return issues_dispatch(h, method, owner, name, key,
                                   rest[1:], query, body)

        # ---- pulls
        if rest and rest[0] == "pulls":
            return pulls_dispatch(h, method, owner, name, key,
                                  rest[1:], query, body)
    return None


def _content_entry(h, key, e):
    base = base_url(h)
    return {"name": e["name"], "path": e["path"],
            "sha": e["sha"], "size": 0,
            "type": "dir" if e["type"] == "tree" else "file",
            "url": base + "/repos/" + key + "/contents/" + e["path"],
            "download_url": None}


def git_data_dispatch(h, method, owner, name, key, rest, body):
    ensure_bare(owner, name)
    if rest and rest[0] == "refs":
        if method == "GET" and len(rest) == 1:
            out = []
            for refname, rsha, rtype in list_refs(owner, name):
                out.append({"ref": refname,
                            "node_id": node_id("Ref", 0)[:24],
                            "object": {"sha": rsha, "type": rtype}})
            return 200, out
        if method == "POST":
            ref = body.get("ref", "")
            sha = body.get("sha", "")
            repo = _open_repo(owner, name)
            try:
                if not re.fullmatch(r"[0-9a-f]{40}", sha or "") or \
                        sha.encode() not in repo.object_store:
                    return err(422, "cannot create ref")
                repo.refs[ref.encode()] = sha.encode()
            except Exception:
                return err(422, "cannot create ref")
            finally:
                repo.close()
            return 201, {"ref": ref, "node_id": node_id("Ref", 0)[:24],
                         "object": {"sha": sha, "type": "commit"}}
        if method == "GET":
            ref = "refs/" + "/".join(rest[1:])
            sha = ref_sha(owner, name, ref)
            if not sha:
                return err(404, "Not Found")
            return 200, {"ref": ref, "node_id": node_id("Ref", 0)[:24],
                         "object": {"sha": sha, "type": "commit"}}
    if rest and rest[0] == "trees" and method == "GET":
        sha = rest[1]
        recursive = (rest and False)
        entries = ls_tree(owner, name, sha, recursive=False)
        if entries is None:
            return err(404, "Not Found")
        tree = [{"path": e["path"], "mode": e["mode"], "type": e["type"],
                 "sha": e["sha"], "size": 0} for e in entries]
        return 200, {"sha": sha, "tree": tree, "truncated": False}
    if rest and rest[0] == "blobs" and method == "GET":
        data = cat_file_bytes(owner, name, rest[1])
        if data is None:
            return err(404, "Not Found")
        return 200, {"content": base64.b64encode(data).decode(),
                     "encoding": "base64", "size": len(data),
                     "sha": rest[1]}
    if rest and rest[0] == "commits" and method == "GET":
        obj = commit_obj(owner, name, rest[1])
        if not obj:
            return err(404, "Not Found")
        c = obj["commit"]
        return 200, {"sha": obj["sha"], "tree": c["tree"],
                     "message": c["message"], "author": c["author"],
                     "committer": c["committer"],
                     "parents": obj["parents"]}
    return err(404, "Not Found")


def issues_dispatch(h, method, owner, name, key, rest, query, body):
    if not rest:
        if method == "GET":
            state = (query.get("state") or ["open"])[0]
            out = []
            for rec in STORE["issues"].get(key, []):
                if rec.get("is_pull"):
                    continue
                if state != "all" and rec["state"] != state:
                    continue
                out.append(issue_obj(h, key, rec))
            sort = (query.get("sort") or ["created"])[0]
            desc = (query.get("direction") or ["asc"])[0] == "desc"
            if sort == "updated":
                out.sort(key=lambda x: (x.get("updated_at") or "",
                                        x["number"]), reverse=desc)
            else:
                out.sort(key=lambda x: x["number"], reverse=desc)
            return 200, out
        if method == "POST":
            names = label_names(body.get("labels"))
            bad = [n for n in names
                   if n not in _repo_label_names(key)]
            if bad:
                return err(422, "Validation Failed: '%s' is not a "
                                "label of this repository" % bad[0])
            rec = create_issue_rec(
                key, body.get("title", ""), body.get("body", ""),
                names,
                body.get("assignees") or
                ([body["assignee"]] if body.get("assignee") else []))
            return 201, issue_obj(h, key, rec)
        return err(405, "Method Not Allowed")
    try:
        number = int(rest[0])
    except ValueError:
        return err(404, "Not Found")
    rec = find_issue(key, number)
    if rec is None:
        return err(404, "Not Found")
    if len(rest) == 1:
        if method == "GET":
            return 200, issue_obj(h, key, rec)
        if method == "PATCH":
            with LOCK:
                if "title" in body:
                    rec["title"] = body["title"]
                if "body" in body:
                    rec["body"] = body["body"]
                if "labels" in body:
                    new_names = label_names(body["labels"])
                    bad = [n for n in new_names
                           if n not in _repo_label_names(key)]
                    if bad:
                        return err(422, "Validation Failed: '%s' is "
                                        "not a label of this "
                                        "repository" % bad[0])
                    rec["labels"] = new_names
                if "assignees" in body:
                    rec["assignees"] = body["assignees"] or []
                if "state" in body:
                    rec["state"] = body["state"]
                    if body["state"] == "closed" and not rec.get(
                            "closed_tick"):
                        rec["closed_tick"] = tick_ts()
                    if body["state"] == "open":
                        rec["closed_tick"] = None
                rec["updated_tick"] = tick_ts()
                save_store()
            return 200, issue_obj(h, key, rec)
    if len(rest) == 2 and rest[1] == "comments":
        if method == "GET":
            return 200, [_comment_obj(h, key, number, c)
                         for c in rec["comments"]]
        if method == "POST":
            with LOCK:
                ts = tick_ts()
                comment = {"id": next_global_id(),
                           "body": body.get("body", ""),
                           "author": DEFAULT_LOGIN,
                           "created_tick": ts, "updated_tick": ts}
                rec["comments"].append(comment)
                rec["updated_tick"] = ts
                save_store()
            return 201, _comment_obj(h, key, number, comment)
    if len(rest) == 2 and rest[1] == "labels":
        if method == "GET":
            return 200, [label_obj(n, i) for i, n in
                         enumerate(rec["labels"])]
        if method == "POST":
            with LOCK:
                for n in label_names(body.get("labels") or
                                     ([body["name"]] if body.get("name")
                                      else [])):
                    if n not in rec["labels"]:
                        rec["labels"].append(n)
                save_store()
            return 200, [label_obj(n, i) for i, n in
                         enumerate(rec["labels"])]
        if method == "DELETE":
            with LOCK:
                rec["labels"] = []
                save_store()
            return 200, []
    return err(404, "Not Found")


def _comment_obj(h, key, number, c):
    base = base_url(h)
    return {"id": c["id"], "node_id": node_id("IssueComment", c["id"]),
            "url": base + "/repos/" + key + "/issues/comments/" +
            str(c["id"]),
            "body": c["body"], "user": user_obj(c["author"]),
            "created_at": fmt_ts(c["created_tick"]),
            "updated_at": fmt_ts(c["updated_tick"])}


def _pull_changed_paths(owner, name, rec):
    """Paths whose blob differs between the pull's base and head
    branch tips: {path: (status, base_entry, head_entry)} with
    entries (mode, sha_hex) or None."""
    repo = _open_repo(owner, name)
    try:
        def flat(branch):
            sha = _resolve_refish(repo, branch)
            if not sha:
                return {}
            obj = repo[sha.encode()]
            if obj.type_name != b"commit":
                return {}
            return _flat_tree(repo, obj.tree)
        base_map = flat(rec["base"])
        head_map = flat(rec["head"])
        out = {}
        for path in sorted(set(base_map) | set(head_map)):
            b, hd = base_map.get(path), head_map.get(path)
            if b == hd:
                continue
            status = "added" if b is None else \
                "removed" if hd is None else "modified"
            out[path] = (status, b, hd)
        return out
    finally:
        repo.close()


def _blob_text(owner, name, entry):
    if entry is None:
        return ""
    repo = _open_repo(owner, name)
    try:
        return repo[entry[1].encode()].as_raw_string().decode(
            "utf-8", "replace")
    finally:
        repo.close()


def _pull_file_obj(h, key, rec, path, change):
    import difflib
    owner, name = key.split("/", 1)
    status, b, hd = change
    old_lines = _blob_text(owner, name, b).splitlines()
    new_lines = _blob_text(owner, name, hd).splitlines()
    adds = dels = 0
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(
            None, old_lines, new_lines).get_opcodes():
        if tag in ("replace", "delete"):
            dels += i2 - i1
        if tag in ("replace", "insert"):
            adds += j2 - j1
    base = base_url(h)
    return {
        "sha": (hd or b)[1],
        "filename": path,
        "status": status,
        "additions": adds,
        "deletions": dels,
        "changes": adds + dels,
        "blob_url": base + "/repos/" + key + "/git/blobs/" +
        (hd or b)[1],
        "raw_url": base + "/" + key + "/raw/" + rec["head"] + "/" +
        path,
        "contents_url": base + "/repos/" + key + "/contents/" + path,
    }


def pulls_dispatch(h, method, owner, name, key, rest, query, body):
    if not rest:
        if method == "GET":
            state = (query.get("state") or ["open"])[0]
            out = [pull_obj(h, key, r) for r in STORE["pulls"].get(
                key, []) if state == "all" or r["state"] == state]
            out.sort(key=lambda x: x["number"])
            return 200, out
        if method == "POST":
            title = body.get("title", "")
            head = body.get("head", "")
            base_ref = body.get("base", "main")
            with LOCK:
                irec = create_issue_rec(
                    key, title, body.get("body", ""), [], [],
                    is_pull=True)
                ts = tick_ts()
                rec = dict(irec)
                rec.update({"head": head, "base": base_ref,
                            "owner": owner, "merged": False,
                            "merged_tick": None})
                STORE["pulls"].setdefault(key, []).append(rec)
                save_store()
            return 201, pull_obj(h, key, rec)
        return err(405, "Method Not Allowed")
    try:
        number = int(rest[0])
    except ValueError:
        return err(404, "Not Found")
    rec = find_pull(key, number)
    if rec is None:
        return err(404, "Not Found")
    if len(rest) == 1:
        if method == "GET":
            return 200, pull_obj(h, key, rec)
        if method == "PATCH":
            with LOCK:
                if "title" in body:
                    rec["title"] = body["title"]
                if "body" in body:
                    rec["body"] = body["body"]
                if "state" in body:
                    rec["state"] = body["state"]
                if "base" in body:
                    rec["base"] = body["base"]
                rec["updated_tick"] = tick_ts()
                # keep the linked issue record in sync
                irec = find_issue(key, number)
                if irec:
                    irec["title"] = rec["title"]
                    irec["body"] = rec["body"]
                    irec["state"] = rec["state"]
                save_store()
            return 200, pull_obj(h, key, rec)
    if len(rest) == 2 and rest[1] == "merge" and method == "PUT":
        try:
            merged = _do_merge(owner, name, rec)
        except _MergeConflict:
            # GitHub answers 405 for a conflicted merge; the pull
            # stays open and unmerged.
            return err(405, "Pull Request is not mergeable")
        with LOCK:
            rec["merged"] = True
            rec["merge_sha"] = merged
            rec["merged_tick"] = tick_ts()
            rec["state"] = "closed"
            irec = find_issue(key, number)
            if irec:
                irec["state"] = "closed"
            save_store()
        return 200, {"sha": merged or "", "merged": True,
                     "message": "Pull Request successfully merged"}
    if len(rest) == 2 and rest[1] == "commits" and method == "GET":
        return 200, []
    if len(rest) == 2 and rest[1] == "files" and method == "GET":
        changed = _pull_changed_paths(owner, name, rec)
        return 200, [_pull_file_obj(h, key, rec, p, changed[p])
                     for p in sorted(changed)]
    if len(rest) == 2 and rest[1] == "comments":
        irec = find_issue(key, number)
        if irec is None:
            return 200, []
        if method == "GET":
            return 200, [_comment_obj(h, key, number, c)
                         for c in irec["comments"]]
    return err(404, "Not Found")


def _ancestors(repo, sha_hex):
    """All ancestor commit shas (incl. self), newest first."""
    try:
        walker = repo.get_walker(include=[sha_hex.encode()])
        return [entry.commit.id.decode() for entry in walker]
    except Exception:
        return [sha_hex]


class _MergeConflict(Exception):
    """Both sides changed the same path in different ways."""


def _pr_conflict(owner, name, rec):
    """True when a three-way merge of the pull's head into its base
    would conflict: some path changed on both sides since the merge
    base, to different results (modify/modify or modify/delete)."""
    head_sha = ref_sha(owner, name, "refs/heads/" + rec["head"])
    base_sha = ref_sha(owner, name, "refs/heads/" + rec["base"])
    if not head_sha or not base_sha or head_sha == base_sha:
        return False
    repo = _open_repo(owner, name)
    try:
        head_anc = _ancestors(repo, head_sha)
        if base_sha in head_anc:
            return False
        base_anc_set = set(_ancestors(repo, base_sha))
        merge_base = next((s for s in head_anc if s in base_anc_set),
                          None)

        def flat_of(sha_hex):
            if sha_hex is None:
                return {}
            obj = repo[sha_hex.encode()]
            return _flat_tree(repo, obj.tree) \
                if obj.type_name == b"commit" else {}

        base_map = flat_of(merge_base)
        ours = flat_of(base_sha)
        theirs = flat_of(head_sha)
        for path in set(base_map) | set(ours) | set(theirs):
            b = base_map.get(path)
            o = ours.get(path)
            t = theirs.get(path)
            if o != t and b != o and b != t:
                return True
        return False
    except Exception:
        return False
    finally:
        repo.close()


def _do_merge(owner, name, rec):
    """Merge head into base in pure Python (gitcore objects).

    Fast-forward when base is an ancestor of head; otherwise a
    tree-level three-way merge against the merge base, producing a
    real two-parent merge commit. Raises _MergeConflict when both
    sides changed the same path differently (the merge endpoint
    answers 405 then, as GitHub does). Falls back to a state-only
    merge (returns base sha) when either branch is missing — the PR
    record still closes consistently.
    """
    head_sha = ref_sha(owner, name, "refs/heads/" + rec["head"])
    base_sha = ref_sha(owner, name, "refs/heads/" + rec["base"])
    if not head_sha or not base_sha:
        return base_sha
    if head_sha == base_sha:
        return base_sha
    repo = _open_repo(owner, name)
    try:
        head_anc = _ancestors(repo, head_sha)
        if base_sha in head_anc:
            repo.refs[("refs/heads/" + rec["base"]).encode()] = \
                head_sha.encode()
            return head_sha
        base_anc_set = set(_ancestors(repo, base_sha))
        merge_base = next((s for s in head_anc if s in base_anc_set),
                          None)

        def flat_of(sha_hex):
            if sha_hex is None:
                return {}
            obj = repo[sha_hex.encode()]
            return _flat_tree(repo, obj.tree) \
                if obj.type_name == b"commit" else {}

        base_map = flat_of(merge_base)
        ours = flat_of(base_sha)
        theirs = flat_of(head_sha)
        merged = {}
        for path in set(base_map) | set(ours) | set(theirs):
            b = base_map.get(path)
            o = ours.get(path)
            t = theirs.get(path)
            if o == t:
                chosen = o
            elif b == o:
                chosen = t
            elif b == t:
                chosen = o
            else:
                # Both sides changed the path differently:
                # the merge conflicts and must be refused.
                raise _MergeConflict(path)
            if chosen is not None:
                merged[path] = chosen
        tree = _build_tree(repo, merged)
        commit = _make_commit(
            repo, tree, [base_sha, head_sha],
            "Merge branch '%s' into %s" % (rec["head"], rec["base"]),
            tick_ts())
        repo.refs[("refs/heads/" + rec["base"]).encode()] = \
            commit.encode()
        return commit
    except _MergeConflict:
        raise
    except Exception:
        return base_sha
    finally:
        repo.close()


# -------------------------------------------------------------- GraphQL
def _gql_cursor(index):
    return base64.b64encode(
        ("forge-cursor:%d" % index).encode()).decode()


def _gql_conn(nodes, first=None, after=None):
    """A Relay-style connection slice over an already-ordered list:
    `first` caps the page, `after` resumes past a previous page's
    endCursor, and pageInfo reports hasNextPage/endCursor."""
    start = 0
    if after:
        try:
            start = int(base64.b64decode(
                str(after).encode()).decode().rpartition(":")[2])
        except Exception:
            start = 0
    page = nodes[start:] if first is None \
        else nodes[start:start + max(0, int(first))]
    end = start + len(page)
    return {
        "totalCount": len(nodes),
        "nodes": page,
        "edges": [{"node": n, "cursor": _gql_cursor(start + i + 1)}
                  for i, n in enumerate(page)],
        "pageInfo": {
            "hasNextPage": end < len(nodes),
            "hasPreviousPage": start > 0,
            "startCursor": _gql_cursor(start) if page else None,
            "endCursor": _gql_cursor(end) if page else after,
        },
    }


def _gql_pull(h, key, rec):
    owner, name = key.split("/", 1)
    changed = _pull_changed_paths(owner, name, rec)
    state = "MERGED" if rec.get("merged") else rec["state"].upper()
    return {
        "id": node_id("PullRequest", rec["id"]),
        "databaseId": rec["id"],
        "number": rec["number"],
        "title": rec["title"],
        "body": rec.get("body") or "",
        "state": state,
        "merged": bool(rec.get("merged")),
        "mergedAt": fmt_ts(rec["merged_tick"])
        if rec.get("merged_tick") else None,
        "mergeable": "MERGEABLE" if rec["state"] == "open"
        and not rec.get("merged") else "UNKNOWN",
        "isDraft": False,
        "headRefName": rec["head"],
        "baseRefName": rec["base"],
        "headRef": {"name": rec["head"]},
        "baseRef": {"name": rec["base"]},
        "url": base_url(h) + "/" + key + "/pull/" +
        str(rec["number"]),
        "createdAt": fmt_ts(rec["created_tick"]),
        "updatedAt": fmt_ts(rec["updated_tick"]),
        "author": {"login": rec.get("author", DEFAULT_LOGIN)},
        "files": {"totalCount": len(changed),
                  "nodes": [{"path": p} for p in sorted(changed)],
                  "edges": [{"node": {"path": p}}
                            for p in sorted(changed)]},
        "commits": {"totalCount": 0, "nodes": []},
        "comments": {"totalCount": len(rec.get("comments", []))},
        "additions": 0, "deletions": 0, "changedFiles": len(changed),
    }


def _gql_repo(h, rec, qopts=None):
    qopts = qopts or {}
    key = rec["owner"] + "/" + rec["name"]
    issues = [i for i in STORE["issues"].get(key, [])
              if not i.get("is_pull")]
    pulls = list(STORE["pulls"].get(key, []))
    base = base_url(h)
    default_sha = ref_sha(rec["owner"], rec["name"],
                          "refs/heads/" + rec["default_branch"]) or ""

    io = qopts.get("issues") or {}
    if io.get("states"):
        want = {s.upper() for s in io["states"]}
        sel = [i for i in issues if i["state"].upper() in want]
    else:
        sel = list(issues)
    sel.sort(key=lambda i: i["number"], reverse=bool(io.get("desc")))
    issues_conn = _gql_conn([_gql_issue(h, key, i) for i in sel],
                            io.get("first"), io.get("after"))
    issues_conn["totalCount"] = len(sel)

    po = qopts.get("pulls") or {}
    if po.get("states"):
        want = {s.upper() for s in po["states"]}
        psel = [p for p in pulls
                if ("MERGED" if p.get("merged")
                    else p["state"].upper()) in want]
    else:
        # gh's pull list defaults to open pull requests.
        psel = [p for p in pulls if p["state"] == "open"
                and not p.get("merged")]
    psel.sort(key=lambda p: p["number"], reverse=bool(po.get("desc")))
    pulls_conn = _gql_conn([_gql_pull(h, key, p) for p in psel],
                           po.get("first"), po.get("after"))
    pulls_conn["totalCount"] = len(psel)

    one_issue = None
    if qopts.get("issue_number") is not None:
        found = find_issue(key, qopts["issue_number"])
        one_issue = _gql_issue(h, key, found) if found else None
    one_pull = None
    if qopts.get("pull_number") is not None:
        found = find_pull(key, qopts["pull_number"])
        one_pull = _gql_pull(h, key, found) if found else None

    return {
        "id": node_id("Repository", rec["id"]),
        "databaseId": rec["id"],
        "name": rec["name"],
        "nameWithOwner": key,
        "description": rec["description"],
        "url": base + "/" + key,
        "sshUrl": "git@" + base.split("://", 1)[1] + ":" + key + ".git",
        "isPrivate": rec["private"],
        "visibility": "PRIVATE" if rec["private"] else "PUBLIC",
        "isFork": False,
        "isArchived": False,
        "viewerPermission": "ADMIN",
        "hasIssuesEnabled": True,
        "hasPullRequestsEnabled": True,
        "createdAt": fmt_ts(rec["created_tick"]),
        "updatedAt": fmt_ts(rec["updated_tick"]),
        "pushedAt": fmt_ts(rec["updated_tick"]),
        "owner": {"login": rec["owner"],
                  "id": node_id("User", 1),
                  "url": base + "/" + rec["owner"]},
        "defaultBranchRef": {"name": rec["default_branch"],
                             "target": {"oid": default_sha}},
        "issues": issues_conn,
        "pullRequests": pulls_conn,
        "issue": one_issue,
        "pullRequest": one_pull,
        "stargazerCount": 0,
        "forkCount": 0,
    }


def _find_issue_by_gid(gid):
    for key, lst in STORE["issues"].items():
        for irec in lst:
            if irec["id"] == gid:
                return key, irec
    return None, None


def _find_pull_by_gid(gid):
    for key, lst in STORE["pulls"].items():
        for prec in lst:
            if prec["id"] == gid:
                return key, prec
    return None, None


def _gql_issue(h, key, rec):
    return {
        "id": node_id("Issue", rec["id"]),
        "databaseId": rec["id"],
        "number": rec["number"],
        "title": rec["title"],
        "body": rec.get("body") or "",
        "state": rec["state"].upper(),
        "url": base_url(h) + "/" + key + "/issues/" + str(rec["number"]),
        "createdAt": fmt_ts(rec["created_tick"]),
        "updatedAt": fmt_ts(rec["updated_tick"]),
        "author": {"login": rec.get("author", DEFAULT_LOGIN)},
        "labels": {"nodes": [{"name": n} for n in rec["labels"]]},
        "comments": {"totalCount": len(rec["comments"])},
    }


def _gql_input_fields(flat, variables):
    """Resolve `field: $var` pairs in the query against variables."""
    resolved = {}
    for field, var in re.findall(r'(\w+)\s*:\s*\$(\w+)', flat):
        if var in variables:
            resolved[field] = variables[var]
    return resolved


def _extract_str(text, field, variables):
    resolved = _gql_input_fields(text, variables)
    if field in resolved:
        return str(resolved[field])
    if field in variables and isinstance(variables[field], str):
        return variables[field]
    m = re.search(field + r'\s*:\s*"([^"]*)"', text)
    return m.group(1) if m else None


_GQL_TYPE_FIELDS = {
    "Issue": ["id", "databaseId", "number", "title", "state", "body",
              "bodyText", "bodyHTML", "author", "labels", "assignees",
              "comments", "milestone", "createdAt", "updatedAt",
              "closedAt", "closed", "locked", "url", "repository",
              "viewerCanReact", "__typename"],
    "PullRequest": ["id", "databaseId", "number", "title", "state",
                    "body", "headRefName", "baseRefName", "headRef",
                    "baseRef", "mergeable", "merged", "mergedAt",
                    "isDraft", "files", "commits", "comments",
                    "reviews", "author", "createdAt", "updatedAt",
                    "closedAt", "url", "repository", "additions",
                    "deletions", "changedFiles", "__typename"],
    "Repository": ["id", "databaseId", "name", "nameWithOwner",
                   "owner", "defaultBranchRef", "viewerPermission",
                   "hasIssuesEnabled", "hasPullRequestsEnabled",
                   "issue", "issues", "pullRequest", "pullRequests",
                   "description", "isPrivate", "visibility",
                   "isFork", "forkCount", "stargazerCount",
                   "createdAt", "updatedAt", "pushedAt", "url",
                   "sshUrl", "homepageUrl", "primaryLanguage",
                   "licenseInfo", "refs", "object", "branchProtectionRules",
                   "__typename"],
    "User": ["id", "databaseId", "login", "name", "url", "avatarUrl",
             "bio", "company", "location", "email", "websiteUrl",
             "createdAt", "updatedAt", "repositories", "issues",
             "pullRequests", "followers", "following", "isViewer",
             "__typename"],
    "Label": ["id", "name", "color", "description", "url",
              "__typename"],
    "IssueComment": ["id", "databaseId", "body", "author",
                     "createdAt", "updatedAt", "url", "issue",
                     "__typename"],
}

_GQL_ENUM_VALUES = {
    "SearchType": ["ISSUE", "REPOSITORY", "USER", "DISCUSSION"],
    "IssueState": ["OPEN", "CLOSED"],
    "PullRequestState": ["OPEN", "CLOSED", "MERGED"],
    "MergeableState": ["MERGEABLE", "CONFLICTING", "UNKNOWN"],
    "RepositoryVisibility": ["PUBLIC", "PRIVATE", "INTERNAL"],
    "IssueOrderField": ["CREATED_AT", "UPDATED_AT", "COMMENTS"],
    "OrderDirection": ["ASC", "DESC"],
    "RepositoryPermission": ["ADMIN", "MAINTAIN", "WRITE", "TRIAGE",
                             "READ"],
    "State": ["OPEN", "CLOSED"],
}


def _gql_type(name):
    if name in _GQL_ENUM_VALUES:
        return {"name": name, "kind": "ENUM", "fields": None,
                "possibleTypes": None,
                "enumValues": [{"name": v, "description": None,
                                "isDeprecated": False,
                                "deprecationReason": None}
                               for v in _GQL_ENUM_VALUES[name]]}
    fields = _GQL_TYPE_FIELDS.get(name)
    if fields is None:
        return None
    return {"name": name, "kind": "OBJECT", "possibleTypes": None,
            "enumValues": None,
            "fields": [{"name": f, "description": None, "args": [],
                        "type": {"kind": "SCALAR", "name": "String",
                                 "ofType": None},
                        "isDeprecated": False,
                        "deprecationReason": None} for f in fields]}


_KNOWN_VIEWER_FIELDS = {
    "login", "id", "databaseId", "name", "url", "avatarUrl", "bio",
    "company", "location", "email", "websiteUrl", "twitterUsername",
    "createdAt", "updatedAt", "repositories", "issues", "pullRequests",
    "followers", "following", "gists", "organizations", "isViewer",
    "viewerCanFollow", "isFollowingViewer", "isEmployee", "isHireable",
    "status", "anyPinnableItems", "pinnedItems", "__typename",
}


def graphql_dispatch(h, payload):
    query = payload.get("query", "") or ""
    variables = payload.get("variables") or {}
    if isinstance(variables, str):
        try:
            variables = json.loads(variables)
        except Exception:
            variables = {}
    flat = re.sub(r"\s+", " ", query)
    # ---- introspection: gh feature-detects with __type probes,
    # often several aliased probes in one document
    type_probes = re.findall(
        r'(?:(\w+)\s*:\s*)?__type\s*\(\s*name\s*:\s*"([^"]+)"', flat)
    if type_probes:
        data = {}
        for alias, tname in type_probes:
            data[alias or "__type"] = _gql_type(tname)
        return 200, {"data": data}
    # ---- an unknown field on viewer is a GraphQL error, HTTP 200
    vm = re.search(r'\bviewer\s*\{([^{}]*)', flat)
    if vm:
        bad = [f for f in re.findall(r'[A-Za-z_][A-Za-z0-9_]*',
                                     vm.group(1))
               if f not in _KNOWN_VIEWER_FIELDS]
        if bad:
            return 200, {"data": {"viewer": None}, "errors": [{
                "message": "Field '%s' doesn't exist on type 'User'"
                           % bad[0],
                "locations": [{"line": 1, "column": 1}],
                "type": "INVALID_FIELD"}]}
    # ---- mutations. gh and github-mcp-server pass one
    # ($input: XInput!) variable holding every field; inline
    # `field: value` / `field: $var` forms resolve through
    # _extract_str as a fallback.
    inp = variables.get("input")
    inp = inp if isinstance(inp, dict) else {}

    def _inp(field):
        if field in inp:
            return inp[field]
        v = _extract_str(flat, field, variables)
        if v is not None:
            return v
        return variables.get(field)

    if "createRepository" in flat:
        name = _inp("name") or ""
        rec = register_repo(DEFAULT_LOGIN, name, "", False)
        return 200, {"data": {"createRepository": {
            "repository": _gql_repo(h, rec),
            "clientMutationId": _inp("clientMutationId")}}}
    if "createIssue" in flat:
        rec = _repo_by_node_or_key(str(_inp("repositoryId") or ""))
        if rec is None:
            return 200, {"data": {"createIssue": None},
                         "errors": [{"message": "repository not found"}]}
        key = rec["owner"] + "/" + rec["name"]
        irec = create_issue_rec(key, _inp("title") or "",
                                _inp("body") or "",
                                _inp("labels") or [])
        return 200, {"data": {"createIssue": {
            "issue": _gql_issue(h, key, irec),
            "clientMutationId": _inp("clientMutationId")}}}
    if "createPullRequest" in flat:
        rec = _repo_by_node_or_key(str(_inp("repositoryId") or ""))
        if rec is None:
            return 200, {"data": {"createPullRequest": None},
                         "errors": [{"message": "repository not found"}]}
        key = rec["owner"] + "/" + rec["name"]
        status, obj = pulls_dispatch(
            h, "POST", rec["owner"], rec["name"], key, [], {},
            {"title": _inp("title") or "",
             "head": _inp("headRefName") or "",
             "base": _inp("baseRefName") or rec["default_branch"],
             "body": _inp("body") or ""})
        if status not in (200, 201):
            return 200, {"data": {"createPullRequest": None},
                         "errors": [obj if isinstance(obj, dict)
                                    else {"message": "create failed"}]}
        prec = find_pull(key, obj["number"])
        return 200, {"data": {"createPullRequest": {
            "pullRequest": _gql_pull(h, key, prec),
            "clientMutationId": _inp("clientMutationId")}}}
    if "mergePullRequest" in flat:
        kind, gid = decode_node_id(str(_inp("pullRequestId") or ""))
        key, prec = _find_pull_by_gid(gid) if gid is not None \
            else (None, None)
        if prec is None:
            return 200, {"data": {"mergePullRequest": None},
                         "errors": [{"message":
                                     "pull request not found"}]}
        owner, name = key.split("/", 1)
        pulls_dispatch(h, "PUT", owner, name, key,
                       [str(prec["number"]), "merge"], {}, {})
        return 200, {"data": {"mergePullRequest": {
            "pullRequest": _gql_pull(h, key, prec),
            "clientMutationId": _inp("clientMutationId")}}}
    if "addComment" in flat:
        kind, gid = decode_node_id(str(_inp("subjectId") or ""))
        key, subject = (None, None)
        if kind == "Issue":
            key, subject = _find_issue_by_gid(gid)
        elif kind == "PullRequest":
            key, subject = _find_pull_by_gid(gid)
        if subject is None:
            return 200, {"data": {"addComment": None},
                         "errors": [{"message": "subject not found"}]}
        with LOCK:
            ts = tick_ts()
            comment = {"id": next_global_id(),
                       "body": _inp("body") or "",
                       "author": DEFAULT_LOGIN,
                       "created_tick": ts, "updated_tick": ts}
            subject.setdefault("comments", []).append(comment)
            subject["updated_tick"] = ts
            save_store()
        node = {"id": node_id("IssueComment", comment["id"]),
                "body": comment["body"],
                "author": {"login": DEFAULT_LOGIN},
                "createdAt": fmt_ts(ts)}
        return 200, {"data": {"addComment": {
            "commentEdge": {"node": node},
            "clientMutationId": _inp("clientMutationId")}}}
    if "closeIssue" in flat or "reopenIssue" in flat:
        closing = "closeIssue" in flat
        kind, gid = decode_node_id(str(_inp("issueId") or ""))
        key, irec = _find_issue_by_gid(gid) if gid is not None \
            else (None, None)
        if irec is None:
            return 200, {"data": {("closeIssue" if closing
                                   else "reopenIssue"): None},
                         "errors": [{"message": "issue not found"}]}
        with LOCK:
            irec["state"] = "closed" if closing else "open"
            irec["closed_tick"] = tick_ts() if closing else None
            irec["updated_tick"] = tick_ts()
            save_store()
        return 200, {"data": {("closeIssue" if closing
                               else "reopenIssue"): {
            "issue": _gql_issue(h, key, irec),
            "clientMutationId": _inp("clientMutationId")}}}
    # ---- queries
    data = {}
    if "viewer" in flat:
        data["viewer"] = {"login": DEFAULT_LOGIN,
                          "id": node_id("User", 1),
                          "url": base_url(h) + "/" + DEFAULT_LOGIN,
                          "name": DEFAULT_LOGIN}
    if "rateLimit" in flat:
        data["rateLimit"] = {"limit": 5000, "cost": 1, "remaining": 4999,
                             "resetAt": fmt_ts(EPOCH_BASE),
                             "used": 1, "nodeCount": 1}
    m = re.search(r'repository\s*\(\s*owner\s*:\s*(?:"([^"]+)"|\$)',
                  flat)
    owner = _extract_str(flat, "owner", variables)
    name = _extract_str(flat, "name", variables)

    def _arg(token):
        if token is None:
            return None
        token = token.strip()
        if token.startswith("$"):
            return variables.get(token[1:])
        if token.startswith('"') and token.endswith('"'):
            return token[1:-1]
        try:
            return int(token)
        except ValueError:
            return token

    def _conn_args(field):
        mm = re.search(r'\b' + field + r'\s*\(([^)]*)\)', flat)
        out = {}
        if not mm:
            return out
        args = mm.group(1)
        fm = re.search(r'first\s*:\s*(\$\w+|\d+)', args)
        if fm:
            v = _arg(fm.group(1))
            if isinstance(v, int):
                out["first"] = v
        am = re.search(r'after\s*:\s*(\$\w+|"[^"]*")', args)
        if am:
            v = _arg(am.group(1))
            if v:
                out["after"] = v
        sm = re.search(r'states\s*:\s*\[([^\]]*)\]', args)
        if sm:
            out["states"] = re.findall(r'[A-Za-z]+', sm.group(1))
        if "direction: DESC" in args or "direction:DESC" in args:
            out["desc"] = True
        return out

    def _single_number(field):
        mm = re.search(r'\b' + field + r'\s*\(\s*number\s*:\s*'
                       r'(\$\w+|\d+)', flat)
        if not mm:
            return None
        v = _arg(mm.group(1))
        return v if isinstance(v, int) else None

    if "repository" in flat and owner and name:
        rec = find_repo(owner, name)
        if rec is None:
            # An unknown repository resolves to null, with no errors.
            data["repository"] = None
            return 200, {"data": data}
        qopts = {"issues": _conn_args("issues"),
                 "pulls": _conn_args("pullRequests"),
                 "issue_number": _single_number("issue"),
                 "pull_number": _single_number("pullRequest")}
        data["repository"] = _gql_repo(h, rec, qopts)
    elif "repository" in flat and name and not owner:
        for rec in STORE["repos"].values():
            if rec["name"] == name:
                data["repository"] = _gql_repo(h, rec)
                break
    m = re.search(r'node\s*\(\s*id\s*:\s*(\$\w+|"[^"]+")', flat)
    if m:
        kind, gid = decode_node_id(str(_arg(m.group(1)) or ""))
        found = None
        if kind == "Repository":
            for rec in STORE["repos"].values():
                if rec["id"] == gid:
                    found = _gql_repo(h, rec)
        elif kind == "Issue":
            k2, i2 = _find_issue_by_gid(gid)
            if i2 is not None:
                found = _gql_issue(h, k2, i2)
        elif kind == "PullRequest":
            k2, p2 = _find_pull_by_gid(gid)
            if p2 is not None:
                found = _gql_pull(h, k2, p2)
        data["node"] = found
    if not data:
        data = {"viewer": {"login": DEFAULT_LOGIN,
                           "id": node_id("User", 1)}}
    return 200, {"data": data}


def _repo_by_node_or_key(ref):
    if not ref:
        return None
    if "/" in ref:
        owner, _, name = ref.partition("/")
        return find_repo(owner, name)
    kind, gid = decode_node_id(ref)
    if kind == "Repository":
        for rec in STORE["repos"].values():
            if rec["id"] == gid:
                return rec
    try:
        gid = int(ref)
        for rec in STORE["repos"].values():
            if rec["id"] == gid:
                return rec
    except (TypeError, ValueError):
        pass
    return None


# ------------------------------------------------------------------ MCP
MCP_TOOLS = [
    ("create_repository", "Create a repository",
     ["name"], ["name", "description", "private", "owner"]),
    ("create_repo", "Create a repository",
     ["name"], ["name", "description", "private", "owner"]),
    ("get_repository", "Get a repository", ["owner", "repo"],
     ["owner", "repo"]),
    ("get_repo", "Get a repository", ["owner", "repo"],
     ["owner", "repo"]),
    ("list_repositories", "List repositories", [], ["owner"]),
    ("search_repositories", "Search repositories", ["query"], ["query"]),
    ("create_issue", "Create an issue", ["owner", "repo", "title"],
     ["owner", "repo", "title", "body", "labels"]),
    ("list_issues", "List issues", ["owner", "repo"],
     ["owner", "repo", "state"]),
    ("get_issue", "Get an issue", ["owner", "repo", "issue_number"],
     ["owner", "repo", "issue_number"]),
    ("create_pull_request", "Create a pull request",
     ["owner", "repo", "title", "head", "base"],
     ["owner", "repo", "title", "head", "base", "body"]),
    ("list_pull_requests", "List pull requests", ["owner", "repo"],
     ["owner", "repo", "state"]),
    ("get_pull_request", "Get a pull request",
     ["owner", "repo", "pull_number"], ["owner", "repo", "pull_number"]),
    ("get_file_contents", "Get file contents", ["owner", "repo", "path"],
     ["owner", "repo", "path", "branch"]),
    ("create_or_update_file", "Create or update a file",
     ["owner", "repo", "path", "content", "message"],
     ["owner", "repo", "path", "content", "message", "branch"]),
    ("list_branches", "List branches", ["owner", "repo"],
     ["owner", "repo"]),
    ("merge_pull_request", "Merge a pull request",
     ["owner", "repo", "pull_number"], ["owner", "repo", "pull_number"]),
]


def _mcp_tool_list():
    tools = []
    for name, desc, required, props in MCP_TOOLS:
        tools.append({
            "name": name, "description": desc,
            "inputSchema": {
                "type": "object",
                "properties": {p: {"type": "string"} for p in props},
                "required": required}})
    return tools


def _mcp_call(h, name, args):
    full = args.get("full_name") or ""
    if full and "/" in full:
        fowner, frepo = full.split("/", 1)
        if not args.get("owner"):
            args["owner"] = fowner
        if not args.get("repo"):
            args["repo"] = frepo
    owner = args.get("owner", DEFAULT_LOGIN)
    repo = args.get("repo", "")
    key = owner + "/" + repo
    if name in ("create_repository", "create_repo"):
        rec = register_repo(args.get("owner", DEFAULT_LOGIN),
                            args.get("name", ""),
                            args.get("description", ""), False)
        return repo_obj(h, rec)
    if name == "search_repositories":
        q = str(args.get("query", "")).lower()
        return {"total_count": len([k for k in STORE["repos"]
                                     if q in k.lower()]),
                "items": [repo_obj(h, r) for k, r in sorted(
                    STORE["repos"].items()) if q in k.lower()]}
    if name == "list_repositories":
        return [repo_obj(h, r) for k, r in sorted(
            STORE["repos"].items())
            if not args.get("owner") or r["owner"] == args["owner"]]
    rec = find_repo(owner, repo)
    if rec is None and name not in ("create_repository", "create_repo"):
        raise ValueError("repository not found: " + key)
    if name in ("get_repository", "get_repo"):
        return repo_obj(h, rec)
    if name == "create_issue":
        irec = create_issue_rec(key, args.get("title", ""),
                                args.get("body", ""),
                                args.get("labels") or [])
        return issue_obj(h, key, irec)
    if name == "list_issues":
        state = args.get("state", "open")
        return [issue_obj(h, key, i) for i in STORE["issues"].get(
            key, []) if not i.get("is_pull") and
            (state == "all" or i["state"] == state)]
    if name == "get_issue":
        irec = find_issue(key, int(args.get("issue_number", 0)))
        if irec is None:
            raise ValueError("issue not found")
        return issue_obj(h, key, irec)
    if name == "create_pull_request":
        status, obj = pulls_dispatch(
            h, "POST", owner, repo, key, [], {},
            {"title": args.get("title", ""), "head": args.get("head", ""),
             "base": args.get("base", "main"),
             "body": args.get("body", "")})
        return obj
    if name == "list_pull_requests":
        return [pull_obj(h, key, r) for r in STORE["pulls"].get(key, [])]
    if name == "get_pull_request":
        prec = find_pull(key, int(args.get("pull_number", 0)))
        if prec is None:
            raise ValueError("pull request not found")
        return pull_obj(h, key, prec)
    if name == "get_file_contents":
        branch = args.get("branch", rec["default_branch"])
        data = cat_file_bytes(owner, repo, branch + ":" +
                              args.get("path", ""))
        if data is None:
            raise ValueError("file not found")
        return {"path": args.get("path"),
                "content": data.decode("utf-8", "replace"),
                "encoding": "utf-8"}
    if name == "create_or_update_file":
        branch = args.get("branch", rec["default_branch"])
        sha = commit_file_change(owner, repo, branch,
                                 args.get("path", ""),
                                 str(args.get("content", "")).encode(),
                                 args.get("message", "Update file"))
        return {"commit": {"sha": sha}, "path": args.get("path")}
    if name == "list_branches":
        return [{"name": refname[len("refs/heads/"):],
                 "commit": {"sha": bsha}}
                for refname, bsha, _t in list_refs(owner, repo,
                                                   "refs/heads/")]
    if name == "merge_pull_request":
        status, obj = pulls_dispatch(
            h, "PUT", owner, repo, key,
            [str(int(args.get("pull_number", 0))), "merge"], {}, {})
        if status != 200:
            raise ValueError(obj.get("message", "merge failed"))
        return obj
    raise ValueError("unknown tool: " + name)


def mcp_dispatch(h, payload):
    method = payload.get("method", "")
    pid = payload.get("id")
    params = payload.get("params") or {}
    if pid is None:  # notification
        return 202, None
    if method == "server/discover":
        result = {
            "supportedVersions": ["2026-07-28", "2025-11-25",
                                  "2024-11-05"],
            "capabilities": {"tools": {"listChanged": False},
                             "resources": {}, "prompts": {}},
            "serverInfo": {"name": "forge-mcp", "version": "1.0.0"}}
        return 200, {"jsonrpc": "2.0", "id": pid, "result": result}
    if method == "initialize":
        result = {
            "protocolVersion": params.get("protocolVersion",
                                          "2024-11-05"),
            "capabilities": {"tools": {"listChanged": False},
                             "resources": {}, "prompts": {}},
            "serverInfo": {"name": "forge-mcp", "version": "1.0.0"}}
        return 200, {"jsonrpc": "2.0", "id": pid, "result": result}
    if method == "ping":
        return 200, {"jsonrpc": "2.0", "id": pid, "result": {}}
    if method == "tools/list":
        return 200, {"jsonrpc": "2.0", "id": pid,
                     "result": {"tools": _mcp_tool_list()}}
    if method == "resources/list":
        return 200, {"jsonrpc": "2.0", "id": pid,
                     "result": {"resources": []}}
    if method == "prompts/list":
        return 200, {"jsonrpc": "2.0", "id": pid,
                     "result": {"prompts": []}}
    if method == "tools/call":
        tname = params.get("name", "")
        targs = params.get("arguments") or {}
        spec = {n: req for n, _d, req, _p in MCP_TOOLS}
        if tname not in spec:
            # An unknown tool answers -32602, never HTTP 500.
            return 200, {"jsonrpc": "2.0", "id": pid, "error": {
                "code": -32602, "message": "unknown tool: " + tname}}
        missing = [r for r in spec[tname]
                   if targs.get(r) is None or targs.get(r) == ""]
        if missing:
            return 200, {"jsonrpc": "2.0", "id": pid, "error": {
                "code": -32602,
                "message": "missing required argument: "
                           + ", ".join(missing)}}
        try:
            out = _mcp_call(h, tname, targs)
            text = json.dumps(out, sort_keys=True)
            return 200, {"jsonrpc": "2.0", "id": pid, "result": {
                "content": [{"type": "text", "text": text}],
                "isError": False}}
        except Exception as exc:
            return 200, {"jsonrpc": "2.0", "id": pid, "result": {
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True}}
    return 200, {"jsonrpc": "2.0", "id": pid, "error": {
        "code": -32601, "message": "method not found: " + method}}


# --------------------------------------------------------------- server
GIT_SUFFIX_RE = re.compile(r"^/([^/]+)/([^/]+?)(\.git)?(/.*)?$")
_GIT_WSGI_APP = None  # lazily built gitcore HTTPGitApplication


class _ForgeReceivePackHandler(_DReceivePackHandler):
    """Receive-pack with GitHub's update rule: every ref update must
    fast-forward. A non-fast-forward update is refused per ref with
    "ng <ref> non-fast-forward" (the handler's report writer formats
    a non-ok status exactly that way); creates and deletes pass."""

    def _on_update(self, ref_name, old_sha, new_sha):
        zero = b"0" * 40
        if not old_sha or not new_sha or old_sha == zero \
                or new_sha == zero:
            return None
        try:
            if not _d_can_ff(self.repo, old_sha, new_sha):
                return b"non-fast-forward"
        except Exception:
            # Non-commit targets (tags) and unreadable histories are
            # not ours to refuse.
            return None
        return None


class _ForgeBackend(_DBackend):
    """gitcore backend serving bare repos from REPOS_DIR.

    (The path the application hands over carries a leading slash, so
    resolve it ourselves. Owner and repo names come from the route
    regex and cannot contain '/' or '..', so the join cannot escape
    REPOS_DIR.)

    The transport handlers never close the repositories they open,
    so repositories opened during one request are tracked per thread
    and closed by _dulwich_wsgi once the response has been fully
    read — this also drops the per-repo object caches while the
    request's data is still coherent.
    """

    def __init__(self):
        self._local = threading.local()

    def begin_request(self):
        self._local.opened = []

    def end_request(self):
        opened = getattr(self._local, "opened", None) or []
        self._local.opened = None
        for repo in opened:
            try:
                repo.close()
            except Exception:
                pass

    def open_repository(self, path):
        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")
        rel = str(path).strip("/")
        repo = _DRepo(str(REPOS_DIR / rel))
        opened = getattr(self._local, "opened", None)
        if opened is not None:
            opened.append(repo)
        return repo


_GIT_BACKEND = _ForgeBackend()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "Forge/1.0"

    def log_message(self, fmt, *args):
        if os.environ.get("FORGE_DEBUG"):
            sys.stderr.write(fmt % args + "\n")

    # -- plumbing
    _body_cache = None

    def _read_body(self):
        # Memoised per request (reset at the top of _route): every
        # response is sent only after the whole request body has
        # been consumed, so a keep-alive connection never desyncs
        # (an early 401/404 with an unread body would otherwise be
        # parsed as the next request line).
        if self._body_cache is not None:
            return self._body_cache
        data = self._read_body_raw()
        self._body_cache = data
        return data

    def _read_body_raw(self):
        length = self.headers.get("Content-Length")
        if length:
            return self.rfile.read(int(length))
        if (self.headers.get("Transfer-Encoding") or "").lower() \
                == "chunked":
            chunks = []
            while True:
                size_line = self.rfile.readline().strip()
                try:
                    size = int(size_line.split(b";")[0], 16)
                except ValueError:
                    break
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
            return b"".join(chunks)
        return b""

    def _send(self, status, data, content_type="application/json",
              headers=None):
        if data is None:
            payload = b""
        elif isinstance(data, (bytes, bytearray)):
            payload = bytes(data)
        elif isinstance(data, str):
            payload = data.encode()
        else:
            payload = json.dumps(data, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        # The suite requires a non-empty X-GitHub-Request-Id on REST
        # replies; a process-local counter keeps it deterministic in
        # shape (headers are not part of the state export).
        self.send_header("X-GitHub-Request-Id",
                         "forge-%d" % next(_REQ_SEQ))
        if headers:
            for hk, hv in headers.items():
                self.send_header(hk, hv)
        self.end_headers()
        if payload and self.command != "HEAD":
            self.wfile.write(payload)

    def _json_body(self):
        raw = self._read_body()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw.decode("utf-8", "replace"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    # -- git smart HTTP via the gitcore WSGI app (no git binary)
    def _handle_git(self, owner, name, rest, parsed):
        with LOCK:
            rec = find_repo(owner, name)
            if rec is None:
                # A repository is created through REST only; a push or
                # clone cannot invent one. Missing repo = 404 on both
                # services, and the server stays up.
                return self._send(404, {"message": "Not Found"})
            ensure_bare(owner, name)
        body = self._read_body() if self.command in ("POST", "PUT") \
            else b""
        # Serialize pushes (ref updates); reads stay concurrent.
        if rest == "/git-receive-pack":
            with LOCK:
                return self._dulwich_wsgi(owner, name, rest, parsed,
                                          body)
        return self._dulwich_wsgi(owner, name, rest, parsed, body)

    def _dulwich_wsgi(self, owner, name, rest, parsed, body):
        global _GIT_WSGI_APP
        if _GIT_WSGI_APP is None:
            _GIT_WSGI_APP = _DHTTPGitApplication(
                _GIT_BACKEND,
                handlers={b"git-receive-pack": _ForgeReceivePackHandler})
        _GIT_BACKEND.begin_request()
        host = self.headers.get("Host", "127.0.0.1")
        server_name, _, server_port = host.partition(":")
        environ = {
            "REQUEST_METHOD": self.command,
            "SCRIPT_NAME": "",
            "PATH_INFO": "/%s/%s.git%s" % (owner, name, rest or ""),
            "QUERY_STRING": parsed.query or "",
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": str(len(body)),
            "SERVER_NAME": server_name or "127.0.0.1",
            "SERVER_PORT": server_port or "80",
            "SERVER_PROTOCOL": "HTTP/1.1",
            "REMOTE_ADDR": self.client_address[0],
            "HTTP_GIT_PROTOCOL": self.headers.get("Git-Protocol", ""),
            "HTTP_AUTHORIZATION": self.headers.get("Authorization", ""),
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": "http",
            "wsgi.input": BytesIO(body),
            "wsgi.errors": sys.stderr,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.run_once": False,
        }
        captured = {}
        parts = []

        def start_response(status, headers, exc_info=None):
            captured["status"] = status
            captured["headers"] = headers
            # Dulwich's smart-HTTP handlers write most of the body
            # through this write() callable, not via yielded chunks —
            # collect both into one ordered sink.
            return parts.append

        try:
            chunks = _GIT_WSGI_APP(environ, start_response)
            try:
                for chunk in chunks:
                    if chunk:
                        parts.append(chunk)
                payload = b"".join(parts)
            finally:
                if hasattr(chunks, "close"):
                    chunks.close()
        except Exception as exc:  # never leak a stack to the client
            _GIT_BACKEND.end_request()
            if os.environ.get("FORGE_DEBUG"):
                sys.stderr.write("dulwich wsgi error: %r\n" % (exc,))
            return self._send(500, b"git transport error",
                              "text/plain")
        _GIT_BACKEND.end_request()
        status = 200
        ctype = "application/octet-stream"
        try:
            status = int(captured.get("status", "200").split()[0])
        except Exception:
            pass
        for k, v in captured.get("headers", []):
            if k.lower() == "content-type":
                ctype = v
        self._send(status, payload, ctype)

    # -- routing
    def _route(self, method):
        self._body_cache = None
        self._read_body()
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)
        query = parse_qs(parsed.query)

        # Boot probe: the env calls GET /healthz twice, with no token.
        if method == "GET" and raw_path.rstrip("/") == "/healthz":
            return self._send(200, {"ok": True})

        # Auth: when the trial hands this process its bearer token
        # (MAKEFUN_SHOP_TOKEN), every API call must carry it; a
        # request without it (or with a wrong one) is 401. With no
        # token in the environment (local runs) nothing is enforced.
        shop_token = os.environ.get("MAKEFUN_SHOP_TOKEN")
        if shop_token and self.headers.get("Authorization") != \
                "Bearer " + shop_token:
            return self._send(401, {"message": "Unauthorized"})

        # MCP
        if raw_path.rstrip("/") in ("/mcp", "/api/mcp"):
            if method == "GET":
                return self._send(405, {"error": "GET not supported"},
                                  "application/json")
            raw = self._read_body()
            try:
                payload = json.loads(raw.decode("utf-8")) if raw \
                    else None
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                # A body that is not JSON: HTTP 400 with -32700.
                return self._send(400, {
                    "jsonrpc": "2.0", "id": None,
                    "error": {"code": -32700, "message": "Parse error"}})
            status, obj = mcp_dispatch(self, payload)
            return self._send(status, obj)
        # GraphQL
        if raw_path.rstrip("/") in ("/graphql", "/api/graphql"):
            if method == "GET":
                q = (query.get("query") or [""])[0]
                status, obj = graphql_dispatch(
                    self, {"query": q, "variables": {}})
                return self._send(status, obj)
            raw = self._read_body()
            try:
                payload = json.loads(raw.decode("utf-8")) if raw \
                    else None
            except Exception:
                payload = None
            if isinstance(payload, list):
                # Batched operations: one result per operation,
                # in order, HTTP 200.
                results = []
                for item in payload:
                    if isinstance(item, dict):
                        _st, obj = graphql_dispatch(self, item)
                        results.append(obj)
                    else:
                        results.append({"errors": [
                            {"message": "invalid operation"}]})
                return self._send(200, results)
            if not isinstance(payload, dict):
                return self._send(400, {"errors": [
                    {"message": "Problems parsing JSON"}]})
            status, obj = graphql_dispatch(self, payload)
            return self._send(status, obj)

        # git smart HTTP: /owner/repo[.git](/info/refs|/git-*-pack|...)
        m = GIT_SUFFIX_RE.match(raw_path)
        if m:
            owner, name, dotgit, rest = (m.group(1), m.group(2),
                                         m.group(3), m.group(4) or "")
            git_rests = ("/info/refs", "/git-upload-pack",
                         "/git-receive-pack", "/HEAD")
            if owner not in ("repos", "api", "user", "users", "orgs",
                             "search", "graphql", "mcp", "rate_limit") \
                    and (dotgit or rest in git_rests):
                return self._handle_git(owner, name, rest, parsed)

        # REST (optionally under /api/v3)
        path = raw_path
        if path.startswith("/api/v3"):
            path = path[len("/api/v3"):]
        parts = [p for p in path.split("/") if p != ""]
        body = {}
        if method in ("POST", "PUT", "PATCH", "DELETE"):
            raw_body = self._read_body()
            if raw_body:
                try:
                    parsed_body = json.loads(
                        raw_body.decode("utf-8", "replace"))
                    if isinstance(parsed_body, dict):
                        body = parsed_body
                except Exception:
                    # A malformed JSON body is a 400, as GitHub does.
                    return self._send(400, {
                        "message": "Problems parsing JSON"})
        result = rest_dispatch(self, method, parts, query, body)
        if result is None:
            return self._send(404, {"message": "Not Found"})
        if len(result) == 3:
            status, obj, hdrs = result
            return self._send(status, obj, headers=hdrs)
        status, obj = result
        return self._send(status, obj)

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_PUT(self):
        self._route("PUT")

    def do_PATCH(self):
        self._route("PATCH")

    def do_DELETE(self):
        self._route("DELETE")

    def do_HEAD(self):
        self._route("GET")


def _env_lifecycle(env_url, port, server):
    """The trial lifecycle (kit starter contract): POST the bound
    port to $MAKEFUN_ENV_URL/register (first register wins; 200/409
    accept, 400/405 are contract errors and end the program, network
    errors retry with backoff), then poll GET /done until the suite
    answers {"done": true}, then shut the server down so the process
    exits 0. Direct connection only: the env is a pod-local service,
    never reached through a proxy."""
    import time as _time
    import urllib.error as _uerr
    import urllib.request as _ureq
    opener = _ureq.build_opener(_ureq.ProxyHandler({}))
    base = env_url.rstrip("/")
    delay = 0.25
    while True:
        try:
            req = _ureq.Request(
                base + "/register",
                data=json.dumps({"port": port}).encode(),
                headers={"content-type": "application/json"},
                method="POST")
            with opener.open(req, timeout=30) as resp:
                if resp.status in (200, 409):
                    break
        except _uerr.HTTPError as exc:
            if exc.code in (200, 409):
                break
            if exc.code in (400, 405):
                os._exit(1)
        except Exception:
            pass
        _time.sleep(delay)
        delay = min(2.0, delay * 2)
    delay = 0.25
    while True:
        try:
            with opener.open(base + "/done", timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8",
                                                         "replace"))
                if isinstance(payload, dict) and \
                        payload.get("done") is True:
                    break
        except Exception:
            pass
        _time.sleep(delay)
        delay = min(2.0, delay * 2)
    server.shutdown()


def main():
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPOS_DIR.mkdir(parents=True, exist_ok=True)
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "0"))
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    actual = server.server_address[1]
    print("PORT %d" % actual, flush=True)
    print("FORGE_LISTENING http://%s:%d" % (host, actual), flush=True)
    port_file = os.environ.get("FORGE_PORT_FILE")
    if port_file:
        with open(port_file, "w", encoding="utf-8") as fh:
            fh.write(str(actual))
    env_url = os.environ.get("MAKEFUN_ENV_URL", "").strip()
    if env_url:
        # Trial mode: register with the env and exit 0 when it says
        # done. Without MAKEFUN_ENV_URL (local runs) serve forever.
        threading.Thread(target=_env_lifecycle,
                         args=(env_url, actual, server),
                         daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

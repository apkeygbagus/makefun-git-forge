[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/apkeygbagus/makefun-git-forge/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/apkeygbagus/makefun-git-forge/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

# Git Forge — make.fun "Build a GitHub-compatible git forge"

Bounty: https://make.fun/bounty/github-core-clone-01
Slug: `github-core-clone-01` · Pool $500 USDC (1st $100, 20 places) ·
**Deadline: 2026-11-21 06:30 UTC = 13:30 WIB** · Entry fee: `null` (GRATIS,
terverifikasi via API requirements resmi).

Standalone, no-UI git forge in **one process**, Python standard library
only — including the git layer itself. **`gitcore.py` is this entry's
own git implementation**, written for this forge: object model
(blob/tree/commit/tag), loose-object + packfile storage (idx v2,
OFS/REF delta resolution, thin-pack completion), refs and config, and
the smart-HTTP transport (`info/refs`, `git-upload-pack` with
want/have negotiation and shallow deepen, `git-receive-pack` with
report-status). It serves git smart HTTP, the GitHub REST v3 API,
GraphQL v4 and MCP from its own on-disk store with real bare git
repositories on disk. **The server never invokes a `git` binary**
(no subprocess calls at all), so it works in the trial pod where no
git binary exists.

## Trial lifecycle (kit contract, implemented in `main.py`)

1. Bind an **ephemeral port** (never 8080).
2. `POST {"port": N}` to `$MAKEFUN_ENV_URL/register` (first register
   wins; 200/409 accept, retries with backoff).
3. Poll `GET $MAKEFUN_ENV_URL/done` until `{"done": true}`.
4. Exit 0.

Plus: `GET /healthz` → `200 {"ok":true}` (the env's boot probe, no
token), and when the trial hands the process its bearer token
(`MAKEFUN_SHOP_TOKEN`) every API call must carry it — a request
without it is **401**. The suite's token owner is the conformance
scenario's viewer login (`adaiversen`, from the kit's published
scenario seed); the forge's own login defaults to it (`FORGE_LOGIN`
overrides for local runs). Without `MAKEFUN_ENV_URL` (local runs) the
server serves standalone until killed. If the repo mount is read-only
(trial `/bot`), the data dir falls back to `/tmp`.

## Run it locally

```bash
python3 main.py            # binds an ephemeral port, prints:
# PORT 51234
# FORGE_LISTENING http://127.0.0.1:51234
```

Env vars: `HOST`, `PORT` (0 = ephemeral), `FORGE_DATA_DIR`,
`FORGE_PORT_FILE`, `FORGE_LOGIN`, `MAKEFUN_ENV_URL`,
`MAKEFUN_SHOP_TOKEN`, `FORGE_DEBUG`.

Clone URL shape: `http://<host>:<port>/<owner>/<repo>.git` ·
REST base: `/api/v3` · GraphQL: `POST /api/graphql` · MCP: `POST /mcp`.

## Implemented surface

| Area | Implemented |
|---|---|
| Git smart HTTP | `info/refs`, `git-upload-pack`, `git-receive-pack` (with and without `.git` suffix) via gitcore's WSGI smart-HTTP app against real bare repos. Clone (incl. `--depth 1` and deepen), push of branches/tags, empty-repo clone. Missing repo = 404 (a push cannot invent a repo). **Every ref update must fast-forward**: a non-fast-forward push is refused per ref with `ng <ref> non-fast-forward` (custom receive-pack handler), mixed pushes apply the fast-forward refs only. |
| REST repos | `POST /user/repos` (incl. `private`, `auto_init`), `GET /user/repos` + `/users/{o}/repos` + `/orgs/{o}/repos` with GitHub-style pagination (`per_page`/`page` + `Link` rel=next/last), `GET/PATCH/DELETE /repos/{o}/{r}`, `GET /user`, `GET /meta`, `GET /rate_limit`, `GET /search/repositories`. Every reply carries `X-GitHub-Request-Id`; a malformed JSON body is a 400. |
| REST contents | `GET` (file → base64, dir → listing), `PUT` (real commit via gitcore objects; response carries the blob `sha` + `encoding: base64`; a stale `sha` on update is a **409**), `DELETE`. |
| REST git data | branches, commits, `git/refs`, `git/trees`, `git/blobs`. |
| REST issues | list with `state` filter + `sort`/`direction`, create (**422 for a label the repo does not have** — GitHub's default label set + API-created labels), get, PATCH (title/body/labels/assignees/state), comments, issue labels, repo labels. Issues and PRs share one numbering. |
| REST pulls | create/get/list (state filters), files (real base↔head tree diff with line counts), commits, comments, `PUT /pulls/{n}/merge`: fast-forward or a real two-parent merge commit; **a conflicted merge is 405** and the pull stays open; `merge_commit_sha` is reported on the pull. |
| GraphQL | `POST /api/graphql` (+ GET): `viewer`, `rateLimit`, `repository(owner, name)` with `viewerPermission`, `hasIssuesEnabled`, `issue(number:)`, `issues(first:, after:, states:, orderBy:)` with Relay `pageInfo` cursors, `pullRequest(number:)` incl. `files`, `pullRequests(first:, states:)`, `node(id:)` for Repository/Issue/PullRequest, `__type` introspection for Issue/PullRequest/Repository/User + enums (SearchType, states, permissions…), batched operations (array body → array of results), unknown repository → `data.repository = null` with no errors, unknown field on viewer → GraphQL errors naming it. Mutations with real effects: `createRepository`, `createIssue`, `createPullRequest`, `mergePullRequest`, `addComment`, `closeIssue`/`reopenIssue` (all `($input: …)` style). |
| MCP | `POST /mcp` JSON-RPC 2.0: `server/discover`, `initialize`, `ping`, `tools/list`, `tools/call`, `resources/list`, `prompts/list`. 16 tools incl. the suite's six — `create_repo`, `get_repo`, `create_issue`, `list_issues`, `create_pull_request`, `merge_pull_request` — acting on the same store as REST. Unknown tool / missing required argument → `-32602`; a non-JSON body → HTTP 400 with `-32700`. |

Determinism: all API timestamps derive from a stored logical tick
counter, never the wall clock; the same operation sequence leaves a
byte-identical `store.json`. Repositories opened for a git request are
closed when the response completes, and gitcore reads pack entries
with short-lived descriptors, so the process survives the 200-push /
50-clone workloads without exhausting file descriptors (the NOFILE
soft limit is raised to the hard limit at startup).

## Local verification (2026-11-17, kit resmi)

- Own end-to-end suite (`test_e2e.py`, server on a PATH with **no git
  binary**): **32/32 passed** — repo CRUD, clone/push/shallow clone,
  PR merge verified on main, diverged two-parent merge, determinism.
- Kit conformance runner (`sdk/run.ts`, the sealed suite's own
  checks): **117/120 checks passed** — git 27/30, repos 15/15,
  issues 15/15, pulls 20/20, protocol 5/5, graphql 20/20, mcp 15/15 →
  **feature 585/600, agent 200/200, working 1**. The 3 misses are the
  git protocol-v2 checks (advertise-v2, ls-refs, ls-refs-prefix):
  gitcore answers protocol v0; the rest of the git checks fall back
  to v0 and pass.
- Perf workloads (kit `--perf`, lokal): **perf 16/200** — clone
  p50 431 ms (16 poin), push 200-op rounds selesai ±9 dtk per round
  (0 poin, ambang ketat), read/write 0 poin. Server tetap hidup dan
  sehat selama bench.

## Format entri (dari kit + API requirements resmi)

- Repo GitHub **publik** milik akun pendaftar, dibuat setelah kompetisi
  dibuka (2026-11-14T06:30Z), lisensi OSI (MIT, `LICENSE` di repo ini).
- Entrypoint `main.py` (Python 3.13) di root repo; **hanya stdlib**,
  tanpa instalasi, tanpa git binary, tanpa layanan luar. Lapisan git
  adalah `gitcore.py` di repo ini — tidak ada kode pihak ketiga.
- Skor: 1.000 pts = Feature 600 + Agent-native 200 + Performance 200;
  hadiah hanya bila skor **di atas house entry**. Maks 5 submit/hari,
  10 total; submit terakhir yang dihitung.
- Submit wajib menyertakan bundle transkrip sesi AI yang membangun
  entri (kit CLI `bundle`); transkrip terekam harness (bukan
  handed-over) agar berhak atas tempat berbayar.
- `.makefun/owner.json` di repo ini membuktikan kepemilikan repo ke
  akun make.fun pendaftar (wallet akun).

## Belum terverifikasi / risiko

- Git protocol v2 (3 check conformance) — lihat di atas.
- Dijalankan lokal di Python 3.12.3; grader memakai Python 3.13
  (kode hanya memakai stdlib ≥3.10, tanpa sintaks 3.13-only).
- Workload performa skala penuh diukur lokal dengan runner kit;
  angka pod bisa berbeda (2 CPU / 1 GiB / wall 900 dtk).

#!/usr/bin/env python3
"""gitcore — a small git implementation written for the Git Forge entry.

Everything the forge needs from git, implemented here from the format
specifications (no third-party git library, no `git` binary):

  * object model: blob / tree / commit / tag parse + serialize, with
    SHA-1 identity exactly as git computes it
  * object store: loose objects (zlib) plus packfile reading
    (idx v2, OFS_DELTA and REF_DELTA resolution), packfile writing
    (plain objects) and pack unpacking into loose objects
  * refs: loose refs, packed-refs, symbolic refs (HEAD)
  * a minimal git config reader/writer
  * commit walking (newest-first) and fast-forward tests
  * smart HTTP transport: ref advertisement, upload-pack (want/have
    negotiation, shallow deepen, side-band-64k) and receive-pack
    (command list, thin packs, report-status), behind a tiny WSGI
    application with a pluggable backend and receive-pack handler,
    mirroring the shape of the interface the forge's HTTP layer was
    already written against.

Object ids are handled as lowercase hex `bytes` throughout, matching
the calling code's expectations.
"""
import hashlib
import heapq
import os
import re
import struct
import zlib
from io import BytesIO

ZERO_SHA = b"0" * 40
TYPE_IDS = {b"commit": 1, b"tree": 2, b"blob": 3, b"tag": 4}
TYPE_NAMES = {num: name for name, num in TYPE_IDS.items()}


def object_id(type_name, raw):
    """The git object id (hex bytes) for a payload."""
    if isinstance(type_name, str):
        type_name = type_name.encode("ascii")
    digest = hashlib.sha1()
    digest.update(b"%s %d\0" % (type_name, len(raw)))
    digest.update(raw)
    return digest.hexdigest().encode("ascii")


def _is_hex40(value):
    return isinstance(value, (bytes, bytearray)) and \
        re.fullmatch(rb"[0-9a-f]{40}", bytes(value)) is not None


# ------------------------------------------------------------------ pkt-line
def pkt_line(payload):
    """Frame one pkt-line (payload bytes, without trailing newline
    unless the caller included one)."""
    return b"%04x" % (len(payload) + 4) + payload


PKT_FLUSH = b"0000"


class PktReader:
    """Sequential pkt-line reader over a request body.

    read_packet() returns the payload bytes, None for a flush packet,
    or b"" at end of input. rest() returns the not-yet-consumed tail
    (the packfile that follows a receive-pack command list).
    """

    def __init__(self, data, pos=0):
        self.data = data
        self.pos = pos

    def read_packet(self):
        if self.pos >= len(self.data):
            return b""
        if self.pos + 4 > len(self.data):
            raise ValueError("truncated pkt-line header")
        try:
            size = int(self.data[self.pos:self.pos + 4], 16)
        except ValueError:
            raise ValueError("invalid pkt-line header")
        if size == 0:
            self.pos += 4
            return None
        if size < 4 or self.pos + size > len(self.data):
            raise ValueError("pkt-line size out of range")
        payload = self.data[self.pos + 4:self.pos + size]
        self.pos += size
        return payload

    def rest(self):
        return self.data[self.pos:]


# ------------------------------------------------------------------ deltas
def _read_delta_varint(buf, pos):
    """The little-endian 7-bit varint used for delta header sizes."""
    value = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, pos


def apply_delta(base, delta):
    """Apply a git delta (as found in packfiles) to base bytes."""
    src_size, pos = _read_delta_varint(delta, 0)
    if src_size != len(base):
        raise ValueError("delta base size mismatch")
    dst_size, pos = _read_delta_varint(delta, pos)
    out = bytearray()
    end = len(delta)
    while pos < end:
        op = delta[pos]
        pos += 1
        if op & 0x80:
            cp_off = 0
            cp_size = 0
            for i in range(4):
                if op & (1 << i):
                    cp_off |= delta[pos] << (8 * i)
                    pos += 1
            for i in range(3):
                if op & (0x10 << i):
                    cp_size |= delta[pos] << (8 * i)
                    pos += 1
            if cp_size == 0:
                cp_size = 0x10000
            out += base[cp_off:cp_off + cp_size]
        elif op:
            out += delta[pos:pos + op]
            pos += op
        else:
            raise ValueError("delta opcode 0 is reserved")
    if len(out) != dst_size:
        raise ValueError("delta result size mismatch")
    return bytes(out)


def _read_pack_varint(buf, pos):
    """Pack object header: type in bits 4-6 of the first byte, size in
    the low 4 bits then 7 bits per continuation byte."""
    byte = buf[pos]
    pos += 1
    type_num = (byte >> 4) & 0x07
    size = byte & 0x0F
    shift = 4
    while byte & 0x80:
        byte = buf[pos]
        pos += 1
        size |= (byte & 0x7F) << shift
        shift += 7
    return type_num, size, pos


def _write_pack_varint(type_num, size):
    first = (type_num << 4) | (size & 0x0F)
    size >>= 4
    out = bytearray()
    if size:
        out.append(first | 0x80)
        while size:
            byte = size & 0x7F
            size >>= 7
            out.append(byte | (0x80 if size else 0))
    else:
        out.append(first)
    return bytes(out)


def _inflate_from(buf, pos):
    """Inflate one zlib stream starting at buf[pos]; returns
    (data, bytes_consumed)."""
    decomp = zlib.decompressobj()
    data = decomp.decompress(bytes(buf[pos:]))
    if not decomp.eof:
        raise ValueError("truncated zlib stream in pack")
    consumed = len(buf) - pos - len(decomp.unused_data)
    return data, consumed


# ------------------------------------------------------------------ objects
def _format_timezone(tz_seconds):
    sign = b"+" if tz_seconds >= 0 else b"-"
    total = abs(int(tz_seconds))
    return b"%s%02d%02d" % (sign, total // 3600, (total % 3600) // 60)


def _parse_timezone(text):
    if len(text) != 5:
        return 0
    sign = -1 if text[:1] == b"-" else 1
    try:
        hours = int(text[1:3])
        minutes = int(text[3:5])
    except ValueError:
        return 0
    return sign * (hours * 3600 + minutes * 60)


def _split_ident(value):
    """b'Name <email> 123456 +0000' -> (b'Name <email>', 123456, 3600).

    The timestamp and timezone are the last two space-separated
    fields; everything before them is the identity. Idents written
    without a timestamp yield time 0."""
    parts = value.rsplit(b" ", 2)
    if len(parts) == 3:
        try:
            return parts[0], int(parts[1]), _parse_timezone(parts[2])
        except ValueError:
            pass
    return value, 0, 0


def _join_ident(ident, when, tz_seconds):
    return b"%s %d %s" % (ident, int(when), _format_timezone(tz_seconds))


class GitObject:
    """Base class: raw payload plus parsed fields.

    as_raw_string() returns the exact stored bytes for an object that
    was only read; once a field is assigned the object is re-serialized
    from its fields (which is byte-exact for the canonical forms git
    itself writes)."""

    type_name = None

    def __init__(self, raw=None):
        self._raw = raw
        self._dirty = raw is None
        self._parse()

    def _parse(self):
        pass

    def _serialize(self):
        return self._raw or b""

    def as_raw_string(self):
        if self._dirty:
            self._raw = self._serialize()
            self._dirty = False
        return self._raw

    def as_raw_chunks(self):
        return [self.as_raw_string()]

    @property
    def id(self):
        return object_id(self.type_name, self.as_raw_string())


class Blob(GitObject):
    type_name = b"blob"

    def _parse(self):
        self._data = self._raw if self._raw is not None else b""

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = bytes(value)
        self._dirty = True

    def _serialize(self):
        return self._data


class TreeEntry:
    __slots__ = ("path", "mode", "sha")

    def __init__(self, path, mode, sha):
        self.path = path          # entry name, bytes
        self.mode = mode          # int, e.g. 0o100644 or 0o40000
        self.sha = sha            # hex bytes


def _tree_sort_key(entry):
    # Git orders tree entries by name, with directories compared as
    # if their name ended in '/'.
    if entry.mode == 0o40000:
        return entry.path + b"/"
    return entry.path


class Tree(GitObject):
    type_name = b"tree"

    def _parse(self):
        self._entries = []
        raw = self._raw or b""
        pos = 0
        while pos < len(raw):
            space = raw.index(b" ", pos)
            mode = int(raw[pos:space], 8)
            nul = raw.index(b"\0", space)
            name = raw[space + 1:nul]
            sha = raw[nul + 1:nul + 21].hex().encode("ascii")
            self._entries.append(TreeEntry(name, mode, sha))
            pos = nul + 21

    def items(self):
        return list(self._entries)

    def add(self, name, mode, sha):
        for entry in self._entries:
            if entry.path == name:
                entry.mode = mode
                entry.sha = sha
                self._dirty = True
                return
        self._entries.append(TreeEntry(name, mode, sha))
        self._dirty = True

    def _serialize(self):
        out = bytearray()
        for entry in sorted(self._entries, key=_tree_sort_key):
            out += b"%o %s\0" % (entry.mode, entry.path)
            out += bytes.fromhex(entry.sha.decode("ascii"))
        return bytes(out)


class Commit(GitObject):
    type_name = b"commit"

    def _parse(self):
        self._tree = None
        self._parents = []
        self._author = b""
        self._author_time = 0
        self._author_timezone = 0
        self._committer = b""
        self._commit_time = 0
        self._commit_timezone = 0
        self._message = b""
        self._extra = []          # [(key, value)] headers we keep verbatim
        raw = self._raw or b""
        head, _, body = raw.partition(b"\n\n")
        self._message = body
        lines = head.split(b"\n") if head else []
        merged = []
        for line in lines:
            if line.startswith(b" ") and merged:
                key, value = merged[-1]
                merged[-1] = (key, value + b"\n" + line)
            else:
                key, _, value = line.partition(b" ")
                merged.append((key, value))
        for key, value in merged:
            if key == b"tree":
                self._tree = value.strip()
            elif key == b"parent":
                self._parents.append(value.strip())
            elif key == b"author":
                self._author, self._author_time, \
                    self._author_timezone = _split_ident(value)
            elif key == b"committer":
                self._committer, self._commit_time, \
                    self._commit_timezone = _split_ident(value)
            else:
                self._extra.append((key, value))

    # -- fields (setters mark the object dirty)
    @property
    def tree(self):
        return self._tree

    @tree.setter
    def tree(self, value):
        self._tree = value
        self._dirty = True

    @property
    def parents(self):
        return list(self._parents)

    @parents.setter
    def parents(self, value):
        self._parents = list(value)
        self._dirty = True

    @property
    def author(self):
        return self._author

    @author.setter
    def author(self, value):
        self._author = value
        self._dirty = True

    @property
    def author_time(self):
        return self._author_time

    @author_time.setter
    def author_time(self, value):
        self._author_time = int(value)
        self._dirty = True

    @property
    def author_timezone(self):
        return self._author_timezone

    @author_timezone.setter
    def author_timezone(self, value):
        self._author_timezone = int(value)
        self._dirty = True

    @property
    def committer(self):
        return self._committer

    @committer.setter
    def committer(self, value):
        self._committer = value
        self._dirty = True

    @property
    def commit_time(self):
        return self._commit_time

    @commit_time.setter
    def commit_time(self, value):
        self._commit_time = int(value)
        self._dirty = True

    @property
    def commit_timezone(self):
        return self._commit_timezone

    @commit_timezone.setter
    def commit_timezone(self, value):
        self._commit_timezone = int(value)
        self._dirty = True

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = bytes(value)
        self._dirty = True

    def _serialize(self):
        out = bytearray()
        out += b"tree %s\n" % self._tree
        for parent in self._parents:
            out += b"parent %s\n" % parent
        out += b"author %s\n" % _join_ident(
            self._author, self._author_time, self._author_timezone)
        out += b"committer %s\n" % _join_ident(
            self._committer, self._commit_time, self._commit_timezone)
        for key, value in self._extra:
            out += b"%s %s\n" % (key, value)
        out += b"\n"
        out += self._message
        return bytes(out)


class Tag(GitObject):
    type_name = b"tag"

    def _parse(self):
        self._object = None
        self._object_type = b""
        self._tag = b""
        self._tagger = b""
        self._tagger_time = 0
        self._tagger_timezone = 0
        self._message = b""
        self._extra = []
        raw = self._raw or b""
        head, _, body = raw.partition(b"\n\n")
        self._message = body
        for line in head.split(b"\n") if head else []:
            key, _, value = line.partition(b" ")
            if key == b"object":
                self._object = value.strip()
            elif key == b"type":
                self._object_type = value.strip()
            elif key == b"tag":
                self._tag = value.strip()
            elif key == b"tagger":
                self._tagger, self._tagger_time, \
                    self._tagger_timezone = _split_ident(value)
            else:
                self._extra.append((key, value))

    @property
    def object(self):
        return self._object

    @object.setter
    def object(self, value):
        self._object = value
        self._dirty = True

    @property
    def object_type(self):
        return self._object_type

    @property
    def tag(self):
        return self._tag

    @property
    def tagger(self):
        return self._tagger

    @property
    def message(self):
        return self._message

    def _serialize(self):
        out = bytearray()
        out += b"object %s\n" % self._object
        out += b"type %s\n" % self._object_type
        out += b"tag %s\n" % self._tag
        if self._tagger:
            out += b"tagger %s\n" % _join_ident(
                self._tagger, self._tagger_time, self._tagger_timezone)
        for key, value in self._extra:
            out += b"%s %s\n" % (key, value)
        out += b"\n"
        out += self._message
        return bytes(out)


_OBJECT_CLASSES = {b"blob": Blob, b"tree": Tree, b"commit": Commit,
                   b"tag": Tag}


def parse_object(type_name, raw):
    if isinstance(type_name, str):
        type_name = type_name.encode("ascii")
    cls = _OBJECT_CLASSES.get(type_name)
    if cls is None:
        raise ValueError("unknown object type %r" % (type_name,))
    return cls(raw)


# ------------------------------------------------------------------ packfiles
class Pack:
    """A packfile plus its v2 index, read lazily.

    The index is parsed into memory once; pack entries are read with
    pread on a short-lived descriptor, so no file handle outlives a
    single lookup (the forge opens many repositories per process and
    must not leak descriptors)."""

    def __init__(self, idx_path, pack_path):
        self.pack_path = pack_path
        with open(idx_path, "rb") as fh:
            idx = fh.read()
        if idx[:4] != b"\xfftOc" or \
                struct.unpack(">I", idx[4:8])[0] != 2:
            raise ValueError("unsupported pack index (need v2)")
        self._fanout = struct.unpack(">256I", idx[8:8 + 1024])
        count = self._fanout[255]
        base = 8 + 1024
        self._shas = [idx[base + 20 * i:base + 20 * i + 20]
                      for i in range(count)]
        off_base = base + 20 * count + 4 * count
        self._offsets = []
        large_base = off_base + 4 * count
        for i in range(count):
            word = struct.unpack(
                ">I", idx[off_base + 4 * i:off_base + 4 * i + 4])[0]
            if word & 0x80000000:
                slot = word & 0x7FFFFFFF
                word = struct.unpack(
                    ">Q", idx[large_base + 8 * slot:
                              large_base + 8 * slot + 8])[0]
            self._offsets.append(word)

    def find_offset(self, sha):
        """Pack offset for a hex sha, or None."""
        raw = bytes.fromhex(sha.decode("ascii"))
        first_byte = raw[0]
        lo = self._fanout[first_byte - 1] if first_byte else 0
        hi = self._fanout[first_byte]
        while lo < hi:
            mid = (lo + hi) // 2
            if self._shas[mid] < raw:
                lo = mid + 1
            elif self._shas[mid] > raw:
                hi = mid
            else:
                return self._offsets[mid]
        return None

    def _read_at(self, offset, size_hint=65536):
        fd = os.open(self.pack_path, os.O_RDONLY)
        try:
            chunks = []
            pos = offset
            while True:
                chunk = os.pread(fd, size_hint, pos)
                if not chunk:
                    break
                chunks.append(chunk)
                pos += len(chunk)
                if len(chunk) < size_hint:
                    break
            return b"".join(chunks)
        finally:
            os.close(fd)

    def get_raw_at(self, offset, store, depth=0):
        """(type_name, raw) for the entry at `offset`, deltas resolved
        against this pack or, for REF_DELTA, against `store`."""
        if depth > 100:
            raise ValueError("delta chain too deep")
        buf = self._read_at(offset)
        type_num, size, pos = _read_pack_varint(buf, 0)
        if type_num == 6:  # OFS_DELTA
            byte = buf[pos]
            pos += 1
            base_off = byte & 0x7F
            while byte & 0x80:
                byte = buf[pos]
                pos += 1
                base_off = ((base_off + 1) << 7) | (byte & 0x7F)
            base_type, base_raw = self.get_raw_at(
                offset - base_off, store, depth + 1)
            delta, _ = _inflate_from(buf, pos)
            raw = apply_delta(base_raw, delta)
            if len(raw) != size:
                raise ValueError("delta result size mismatch")
            return base_type, raw
        if type_num == 7:  # REF_DELTA
            base_sha = buf[pos:pos + 20].hex().encode("ascii")
            pos += 20
            base_type, base_raw = store.get_raw(base_sha)
            delta, _ = _inflate_from(buf, pos)
            raw = apply_delta(base_raw, delta)
            if len(raw) != size:
                raise ValueError("delta result size mismatch")
            return base_type, raw
        raw, _ = _inflate_from(buf, pos)
        if len(raw) != size:
            raise ValueError("pack object size mismatch")
        return TYPE_NAMES[type_num], raw


def build_pack(entries):
    """Serialize [(type_name, raw)] into a complete packfile (no
    deltas — plain objects are always valid)."""
    out = bytearray()
    out += b"PACK" + struct.pack(">II", 2, len(entries))
    for type_name, raw in entries:
        out += _write_pack_varint(TYPE_IDS[type_name], len(raw))
        out += zlib.compress(raw)
    out += hashlib.sha1(bytes(out)).digest()
    return bytes(out)


def unpack_pack(data, store):
    """Unpack a received packfile into `store` as loose objects.

    Handles OFS_DELTA / REF_DELTA, including thin packs whose delta
    bases already live in the store. Returns the object count.
    Raises ValueError on any structural problem."""
    if len(data) < 12 + 20 or data[:4] != b"PACK":
        raise ValueError("not a packfile")
    version, count = struct.unpack(">II", data[4:12])
    if version not in (2, 3):
        raise ValueError("unsupported pack version %d" % version)
    trailer = data[-20:]
    if hashlib.sha1(data[:-20]).digest() != trailer:
        raise ValueError("pack trailer checksum mismatch")
    body = data[:-20]
    pos = 12
    by_offset = {}   # offset -> (type_name, raw)
    resolved = []    # (type_name, raw) in stream order

    def resolve_at(offset, depth=0):
        if offset in by_offset:
            return by_offset[offset]
        if depth > 100:
            raise ValueError("delta chain too deep")
        type_num, size, p = _read_pack_varint(body, offset)
        if type_num == 6:
            byte = body[p]
            p += 1
            base_off = byte & 0x7F
            while byte & 0x80:
                byte = body[p]
                p += 1
                base_off = ((base_off + 1) << 7) | (byte & 0x7F)
            base_type, base_raw = resolve_at(offset - base_off,
                                             depth + 1)
            delta, _ = _inflate_from(body, p)
            raw = apply_delta(base_raw, delta)
            result = (base_type, raw)
        elif type_num == 7:
            base_sha = body[p:p + 20].hex().encode("ascii")
            p += 20
            base_type, base_raw = store.get_raw(base_sha)
            delta, _ = _inflate_from(body, p)
            raw = apply_delta(base_raw, delta)
            result = (base_type, raw)
        else:
            raw, _ = _inflate_from(body, p)
            result = (TYPE_NAMES[type_num], raw)
        if len(result[1]) != size:
            raise ValueError("pack object size mismatch")
        by_offset[offset] = result
        return result

    for _ in range(count):
        offset = pos
        type_num, _size, p = _read_pack_varint(body, pos)
        if type_num == 6:
            byte = body[p]
            p += 1
            while byte & 0x80:
                byte = body[p]
                p += 1
        elif type_num == 7:
            p += 20
        _raw, consumed = _inflate_from(body, p)
        pos = p + consumed
        resolved.append(resolve_at(offset))
    if pos != len(body):
        raise ValueError("pack has trailing garbage")
    for type_name, raw in resolved:
        store.add_raw(type_name, raw)
    return count


# ------------------------------------------------------------------ object store
class ObjectStore:
    """Loose objects plus any packfiles under objects/pack."""

    def __init__(self, path):
        self.path = path
        self._packs = None
        self._raw_cache = {}
        self._obj_cache = {}

    # -- packs
    def _load_packs(self):
        if self._packs is None:
            self._packs = []
            pack_dir = os.path.join(self.path, "pack")
            try:
                names = os.listdir(pack_dir)
            except OSError:
                names = []
            for name in sorted(names):
                if name.endswith(".idx"):
                    try:
                        self._packs.append(Pack(
                            os.path.join(pack_dir, name),
                            os.path.join(pack_dir,
                                         name[:-4] + ".pack")))
                    except Exception:
                        continue
        return self._packs

    def close(self):
        self._packs = None
        self._raw_cache.clear()
        self._obj_cache.clear()

    # -- loose
    def _loose_path(self, sha):
        return os.path.join(self.path, sha[:2].decode("ascii"),
                            sha[2:].decode("ascii"))

    def _read_loose(self, sha):
        with open(self._loose_path(sha), "rb") as fh:
            data = zlib.decompress(fh.read())
        header, _, raw = data.partition(b"\0")
        type_name, _, _size = header.partition(b" ")
        return type_name, raw

    # -- public
    def get_raw(self, sha):
        sha = bytes(sha)
        hit = self._raw_cache.get(sha)
        if hit is not None:
            return hit
        if os.path.exists(self._loose_path(sha)):
            result = self._read_loose(sha)
        else:
            result = None
            for pack in self._load_packs():
                offset = pack.find_offset(sha)
                if offset is not None:
                    result = pack.get_raw_at(offset, self)
                    break
            if result is None:
                raise KeyError(sha.decode("ascii", "replace"))
        self._raw_cache[sha] = result
        return result

    def get(self, sha):
        sha = bytes(sha)
        obj = self._obj_cache.get(sha)
        if obj is None:
            type_name, raw = self.get_raw(sha)
            obj = parse_object(type_name, raw)
            self._obj_cache[sha] = obj
        return obj

    def __contains__(self, sha):
        try:
            sha = bytes(sha)
        except Exception:
            return False
        if not _is_hex40(sha):
            return False
        if sha in self._raw_cache:
            return True
        if os.path.exists(self._loose_path(sha)):
            return True
        for pack in self._load_packs():
            if pack.find_offset(sha) is not None:
                return True
        return False

    def add_raw(self, type_name, raw):
        if isinstance(type_name, str):
            type_name = type_name.encode("ascii")
        sha = object_id(type_name, raw)
        self._raw_cache[sha] = (type_name, raw)
        path = self._loose_path(sha)
        if os.path.exists(path):
            return sha
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp%d" % os.getpid()
        with open(tmp, "wb") as fh:
            fh.write(zlib.compress(
                b"%s %d\0" % (type_name, len(raw)) + raw))
        os.rename(tmp, path)
        return sha

    def add_object(self, obj):
        sha = self.add_raw(obj.type_name, obj.as_raw_string())
        self._obj_cache[sha] = obj
        return sha


# ------------------------------------------------------------------ config
class Config:
    """A minimal git config: ordered (section, key, value) entries.

    Sections are tuples of bytes, e.g. (b"core",) or
    (b"remote", b"origin"); keys and values are bytes."""

    def __init__(self, path=None):
        self._path = path
        self._entries = []
        if path and os.path.exists(path):
            self._load(path)

    def _load(self, path):
        section = None
        with open(path, "rb") as fh:
            text = fh.read().decode("utf-8", "replace")
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith(("#", ";")):
                continue
            if stripped.startswith("["):
                inner = stripped[1:stripped.index("]")]
                if '"' in inner:
                    name, _, sub = inner.partition('"')
                    section = (name.strip().encode(),
                               sub.rstrip('"').encode())
                else:
                    section = (inner.strip().encode(),)
                continue
            if section is None:
                continue
            key, eq, value = stripped.partition("=")
            value = value.strip() if eq else "true"
            self._entries.append(
                (section, key.strip().lower().encode(),
                 value.encode()))

    def get(self, section, key, default=None):
        for sec, k, v in self._entries:
            if sec == tuple(section) and k == key:
                return v
        return default

    def set(self, section, key, value):
        section = tuple(section)
        for i, (sec, k, _v) in enumerate(self._entries):
            if sec == section and k == key:
                self._entries[i] = (section, key, value)
                return
        self._entries.append((section, key, value))

    def write_to_path(self, path=None):
        target = path or self._path
        if target is None:
            raise ValueError("config has no path")
        out = []
        current = None
        for section, key, value in self._entries:
            if section != current:
                current = section
                if len(section) == 1:
                    out.append(b"[%s]" % section[0])
                else:
                    out.append(b'[%s "%s"]' % (section[0], section[1]))
            out.append(b"\t%s = %s" % (key, value))
        with open(target, "wb") as fh:
            fh.write(b"\n".join(out) + b"\n")


# ------------------------------------------------------------------ refs
class RefsContainer:
    """Loose refs backed by files, plus packed-refs reading.

    Names and values are bytes; values are hex object ids. Symbolic
    refs (files whose content is 'ref: <target>') are followed on
    read, as git does."""

    def __init__(self, git_dir):
        self.path = git_dir

    def _ref_path(self, name):
        return os.path.join(self.path, name.decode("utf-8", "replace"))

    def _packed_refs(self):
        packed = {}
        peeled = {}
        path = os.path.join(self.path, "packed-refs")
        try:
            with open(path, "rb") as fh:
                lines = fh.read().splitlines()
        except OSError:
            return packed, peeled
        last = None
        for line in lines:
            if not line or line.startswith(b"#"):
                continue
            if line.startswith(b"^"):
                if last is not None:
                    peeled[last] = line[1:].strip()
                continue
            sha, _, name = line.partition(b" ")
            name = name.strip()
            packed[name] = sha.strip()
            last = name
        return packed, peeled

    def get_symref(self, name):
        """The target of a symbolic ref, or None if `name` is not one
        (or does not exist)."""
        try:
            with open(self._ref_path(name), "rb") as fh:
                content = fh.read().strip()
        except OSError:
            return None
        if content.startswith(b"ref:"):
            return content[4:].strip()
        return None

    def follow(self, name):
        """Resolve a possibly-symbolic ref: (final_name, sha|None)."""
        seen = set()
        current = name
        while current not in seen:
            seen.add(current)
            target = self.get_symref(current)
            if target is None:
                return current, self._read_direct(current)
            current = target
        return current, None

    def _read_direct(self, name):
        try:
            with open(self._ref_path(name), "rb") as fh:
                content = fh.read().strip()
        except OSError:
            content = None
        if content is not None:
            if _is_hex40(content):
                return content
            return None
        packed, _peeled = self._packed_refs()
        return packed.get(name)

    def read_ref(self, name):
        _final, sha = self.follow(name)
        return sha

    def as_dict(self):
        """{refname: sha} for every ref, HEAD included when it
        resolves. Loose refs shadow packed ones."""
        result = {}
        packed, _peeled = self._packed_refs()
        result.update(packed)
        refs_root = os.path.join(self.path, "refs")
        for dirpath, _dirs, files in os.walk(refs_root):
            for fname in files:
                full = os.path.join(dirpath, fname)
                rel = os.path.relpath(full, self.path)
                name = rel.replace(os.sep, "/").encode()
                sha = self._read_direct(name)
                if sha is not None:
                    result[name] = sha
        head = self.read_ref(b"HEAD")
        if head is not None:
            result[b"HEAD"] = head
        return result

    def __setitem__(self, name, sha):
        sha = bytes(sha)
        if not _is_hex40(sha):
            raise ValueError("ref value must be a 40-hex object id")
        path = self._ref_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".lock%d" % os.getpid()
        with open(tmp, "wb") as fh:
            fh.write(sha + b"\n")
        os.rename(tmp, path)

    def __delitem__(self, name):
        self.remove_ref(name)

    def remove_ref(self, name):
        path = self._ref_path(name)
        try:
            os.unlink(path)
        except OSError:
            pass
        # Drop it from packed-refs too, if present there.
        packed_path = os.path.join(self.path, "packed-refs")
        if os.path.exists(packed_path):
            with open(packed_path, "rb") as fh:
                lines = fh.read().splitlines()
            kept = []
            skip_peeled = False
            for line in lines:
                if skip_peeled and line.startswith(b"^"):
                    skip_peeled = False
                    continue
                skip_peeled = False
                _sha, _, refname = line.partition(b" ")
                if refname.strip() == name:
                    skip_peeled = True
                    continue
                kept.append(line)
            with open(packed_path, "wb") as fh:
                fh.write(b"\n".join(kept) + (b"\n" if kept else b""))
        # Prune now-empty parent directories under refs/.
        parent = os.path.dirname(path)
        refs_root = os.path.join(self.path, "refs")
        while parent.startswith(refs_root) and parent != refs_root:
            try:
                os.rmdir(parent)
            except OSError:
                break
            parent = os.path.dirname(parent)

    def set_symbolic_ref(self, name, target):
        path = self._ref_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(b"ref: %s\n" % target)


# ------------------------------------------------------------------ repository
class Walker:
    """Commit walk, newest first (commit-time order, parents after
    children), as the forge's history listings expect."""

    def __init__(self, repo, include, max_entries=None):
        self.repo = repo
        self.include = [bytes(sha) for sha in include]
        self.max_entries = max_entries

    def __iter__(self):
        store = self.repo.object_store
        heap = []
        seen = set()
        counter = [0]

        def push(sha):
            if sha in seen or not _is_hex40(sha):
                return
            seen.add(sha)
            try:
                obj = store.get(sha)
            except KeyError:
                return
            if obj.type_name == b"tag":
                push(obj.object)
                return
            if obj.type_name != b"commit":
                return
            counter[0] += 1
            heapq.heappush(heap, (-obj.commit_time, counter[0], sha))

        for sha in self.include:
            push(sha)
        produced = 0
        while heap:
            if self.max_entries is not None and \
                    produced >= self.max_entries:
                return
            _neg_time, _n, sha = heapq.heappop(heap)
            commit = store.get(sha)
            produced += 1
            yield _WalkEntry(commit)
            for parent in commit.parents:
                push(parent)


class _WalkEntry:
    __slots__ = ("commit",)

    def __init__(self, commit):
        self.commit = commit


class Repo:
    """A bare repository directory (HEAD, objects/, refs/, config)."""

    def __init__(self, path):
        self.path = str(path)
        self.object_store = ObjectStore(
            os.path.join(self.path, "objects"))
        self.refs = RefsContainer(self.path)
        self._config = None

    @classmethod
    def init_bare(cls, path):
        path = str(path)
        for sub in ("objects", "objects/pack", "objects/info",
                    "refs/heads", "refs/tags", "info", "hooks",
                    "branches"):
            os.makedirs(os.path.join(path, sub), exist_ok=True)
        head = os.path.join(path, "HEAD")
        if not os.path.exists(head):
            with open(head, "wb") as fh:
                fh.write(b"ref: refs/heads/main\n")
        config_path = os.path.join(path, "config")
        if not os.path.exists(config_path):
            cfg = Config(config_path)
            cfg.set((b"core",), b"repositoryformatversion", b"0")
            cfg.set((b"core",), b"filemode", b"true")
            cfg.set((b"core",), b"bare", b"true")
            cfg.write_to_path()
        return cls(path)

    def close(self):
        self.object_store.close()

    def __getitem__(self, sha):
        return self.object_store.get(bytes(sha))

    def head(self):
        sha = self.refs.read_ref(b"HEAD")
        if sha is None:
            raise KeyError("HEAD does not point at a commit")
        return sha

    def get_config(self):
        if self._config is None:
            self._config = Config(os.path.join(self.path, "config"))
        return self._config

    def get_walker(self, include, max_entries=None):
        return Walker(self, include, max_entries=max_entries)


def _peel_to_commit(repo, sha):
    obj = repo.object_store.get(sha)
    while obj.type_name == b"tag":
        sha = obj.object
        obj = repo.object_store.get(sha)
    if obj.type_name != b"commit":
        raise ValueError("object is not a commit")
    return sha, obj


def can_fast_forward(repo, old_sha, new_sha):
    """True when old_sha is an ancestor of (or equal to) new_sha."""
    old_sha = bytes(old_sha)
    new_sha = bytes(new_sha)
    if old_sha == new_sha:
        return True
    old_sha, _obj = _peel_to_commit(repo, old_sha)
    new_sha, _obj = _peel_to_commit(repo, new_sha)
    seen = set()
    stack = [new_sha]
    while stack:
        current = stack.pop()
        if current == old_sha:
            return True
        if current in seen:
            continue
        seen.add(current)
        try:
            commit = repo.object_store.get(current)
        except KeyError:
            continue
        if commit.type_name == b"commit":
            stack.extend(commit.parents)
    return False


# ------------------------------------------------------------------ smart HTTP
AGENT_STRING = "gitcore/1.0"
UPLOAD_PACK_CAPS = (
    "side-band-64k ofs-delta shallow "
    "allow-tip-sha1-in-want allow-reachable-sha1-in-want "
    "agent=" + AGENT_STRING)
RECEIVE_PACK_CAPS = (
    "report-status delete-refs side-band-64k quiet ofs-delta "
    "agent=" + AGENT_STRING)


def _peel_fully(store, sha):
    """Follow tag objects to the object they ultimately name."""
    obj = store.get(sha)
    while obj.type_name == b"tag":
        sha = obj.object
        obj = store.get(sha)
    return sha


def advertise_refs(repo, service):
    """The v0 ref advertisement (pkt-lines, without the service
    preamble): every resolvable ref in name order (HEAD sorts first
    when it resolves), the first line carrying the capabilities,
    annotated tags followed by their peeled line.

    An unborn HEAD is not advertised at all: a repository with no
    resolvable refs advertises an empty list, which is how git's own
    tools recognise an empty repository."""
    caps = UPLOAD_PACK_CAPS if service == "git-upload-pack" \
        else RECEIVE_PACK_CAPS
    refs = repo.refs
    head_target = refs.get_symref(b"HEAD")
    if head_target is not None:
        caps += " symref=HEAD:%s" % head_target.decode(
            "utf-8", "replace")
    all_refs = refs.as_dict()
    out = bytearray()
    if not all_refs and service == "git-receive-pack":
        # A push client takes its capabilities from the first
        # advertisement line; with no refs at all, git's convention
        # is a pseudo-ref that exists only to carry them.
        out += pkt_line(b"%s capabilities^{}\0%s\n"
                        % (ZERO_SHA, caps.encode()))
    first = True
    for name in sorted(all_refs):
        sha = all_refs[name]
        if first:
            out += pkt_line(b"%s %s\0%s\n" % (sha, name,
                                              caps.encode()))
            first = False
        else:
            out += pkt_line(b"%s %s\n" % (sha, name))
        try:
            obj = repo.object_store.get(sha)
        except KeyError:
            continue
        if obj.type_name == b"tag":
            try:
                peeled = _peel_fully(repo.object_store, sha)
            except KeyError:
                continue
            out += pkt_line(b"%s %s^{}\n" % (peeled, name))
    out += PKT_FLUSH
    return bytes(out)


def _dumb_refs(repo):
    refs = repo.refs.as_dict()
    out = bytearray()
    for name in sorted(refs):
        out += b"%s\t%s\n" % (refs[name], name)
    return bytes(out)


def _reachable_objects(store, roots, stop_commits=frozenset()):
    """Every object sha reachable from `roots`. Commits listed in
    stop_commits are included but their parents are not traversed
    (the client holds them as shallow boundary: it does not have
    their history)."""
    seen = set()
    stack = [bytes(r) for r in roots]
    while stack:
        sha = stack.pop()
        if sha in seen:
            continue
        try:
            obj = store.get(sha)
        except KeyError:
            continue
        seen.add(sha)
        if obj.type_name == b"commit":
            stack.append(obj.tree)
            if sha not in stop_commits:
                stack.extend(obj.parents)
        elif obj.type_name == b"tree":
            for entry in obj.items():
                stack.append(entry.sha)
        elif obj.type_name == b"tag":
            stack.append(obj.object)
    return seen


def _commit_closure(store, roots):
    """Commit shas reachable from the given commit/tag roots."""
    seen = set()
    stack = [bytes(r) for r in roots]
    while stack:
        sha = stack.pop()
        if sha in seen:
            continue
        try:
            obj = store.get(sha)
        except KeyError:
            continue
        if obj.type_name == b"tag":
            stack.append(obj.object)
            continue
        if obj.type_name != b"commit":
            continue
        seen.add(sha)
        stack.extend(obj.parents)
    return seen


def _include_tree_closure(store, included, have_closure, root):
    """Add a tree/blob/tag-target closure to `included`, skipping
    anything the client already has. Commits found along the way
    (tag targets) are returned for the caller's depth logic."""
    found_commits = []
    stack = [root]
    while stack:
        sha = stack.pop()
        if sha in included or sha in have_closure:
            continue
        try:
            obj = store.get(sha)
        except KeyError:
            continue
        if obj.type_name == b"commit":
            found_commits.append(sha)
            continue
        included.add(sha)
        if obj.type_name == b"tree":
            for entry in obj.items():
                stack.append(entry.sha)
        elif obj.type_name == b"tag":
            stack.append(obj.object)
    return found_commits


def serve_upload_pack(repo, body):
    """One git-upload-pack request -> (status, content_type, bytes).

    v0 negotiation: want/have/done with optional shallow deepen.
    The depth of a deepen request is counted from the wanted tips,
    as git counts it."""
    store = repo.object_store
    reader = PktReader(body)
    wants = []
    haves = []
    client_shallow = []
    depth = None
    deepen_since = None
    deepen_not = []
    caps = set()
    done_seen = False
    while True:
        packet = reader.read_packet()
        if packet == b"":
            break
        if packet is None:
            continue
        line = packet[:-1] if packet.endswith(b"\n") else packet
        if line == b"done":
            done_seen = True
        elif line.startswith(b"want "):
            parts = line[5:].split(b" ")
            wants.append(parts[0])
            if len(parts) > 1:
                caps.update(p for p in parts[1:] if p)
        elif line.startswith(b"have "):
            haves.append(line[5:].strip())
        elif line.startswith(b"shallow "):
            client_shallow.append(line[8:].strip())
        elif line.startswith(b"deepen-since "):
            deepen_since = int(line[13:].strip())
        elif line.startswith(b"deepen-not "):
            deepen_not.append(line[11:].strip())
        elif line.startswith(b"deepen "):
            depth = int(line[7:].strip())
        # 'done' and anything unrecognised need no bookkeeping: the
        # request body is the whole negotiation.
    sideband = b"side-band-64k" in caps
    if not wants:
        return 200, "application/x-git-upload-pack-result", \
            pkt_line(b"NAK\n") + PKT_FLUSH
    for want in wants:
        if want not in store:
            raise ValueError("want %s is not an object here"
                             % want.decode("ascii", "replace"))
    shallow_set = set(client_shallow)
    have_closure = _reachable_objects(store, haves,
                                      stop_commits=shallow_set)
    not_commits = set()
    for tip in deepen_not:
        sha = tip if _is_hex40(tip) else repo.refs.read_ref(tip)
        if sha is not None:
            not_commits |= _commit_closure(store, [sha])

    # Inclusion: commits by distance from the wants, everything else
    # (trees, blobs, tags) by full closure. A commit the client
    # already has is never sent, but the walk still passes through
    # it — a deepen request extends a shallow boundary precisely by
    # walking from a wanted tip the client holds into the parents
    # it lacks. The walk never escapes the have closure except
    # across a shallow boundary, and it crosses one of those only
    # when the request deepens.
    from collections import deque
    included = set()
    distances = {}
    queue = deque()

    def offer_commit(sha, dist):
        if sha in distances and distances[sha] <= dist:
            return
        distances[sha] = dist
        queue.append((sha, dist))

    for want in wants:
        obj = store.get(want)
        if obj.type_name == b"commit":
            offer_commit(want, 1)
        else:
            for commit_sha in _include_tree_closure(
                    store, included, have_closure, want):
                offer_commit(commit_sha, 1)
    deepening = depth is not None or deepen_since is not None
    while queue:
        sha, dist = queue.popleft()
        if distances.get(sha) != dist:
            continue
        if depth is not None and dist > depth:
            continue
        obj = store.get(sha)
        if sha not in have_closure \
                and sha not in not_commits \
                and not (deepen_since is not None
                         and obj.commit_time <= deepen_since):
            included.add(sha)
            _include_tree_closure(store, included, have_closure,
                                  obj.tree)
        if sha in shallow_set and not deepening:
            continue
        for parent in obj.parents:
            offer_commit(parent, dist + 1)

    # Shallow bookkeeping from the client's point of view.
    new_shallow = []
    for sha in included:
        obj = store.get(sha)
        if obj.type_name != b"commit":
            continue
        for parent in obj.parents:
            if parent in included or parent in have_closure:
                continue
            try:
                store.get(parent)
            except KeyError:
                continue
            new_shallow.append(sha)
            break
    unshallow = []
    for sha in shallow_set:
        if sha not in included and sha not in have_closure:
            continue
        try:
            obj = store.get(sha)
        except KeyError:
            continue
        if obj.type_name != b"commit":
            continue
        if all(p in included or p in have_closure
               for p in obj.parents):
            unshallow.append(sha)

    out = bytearray()
    for sha in sorted(new_shallow):
        out += pkt_line(b"shallow %s\n" % sha)
    for sha in sorted(unshallow):
        out += pkt_line(b"unshallow %s\n" % sha)
    shallow_negotiation = depth is not None \
        or deepen_since is not None or bool(client_shallow)
    if shallow_negotiation or new_shallow or unshallow:
        # The shallow section is flush-terminated, separately from
        # the ACK/NAK section that follows it.
        out += PKT_FLUSH
    if shallow_negotiation and not done_seen:
        # Stateless RPC splits a shallow fetch into two requests:
        # the first (no 'done' yet) is answered with the shallow
        # section alone; the client repeats the whole stream with
        # 'done' appended and only then expects the pack.
        return 200, "application/x-git-upload-pack-result", bytes(out)
    entries = []
    for sha in included:
        entries.append(store.get_raw(sha))
    pack = build_pack(entries)
    out += pkt_line(b"NAK\n")
    if sideband:
        for start in range(0, len(pack), 65515):
            out += pkt_line(b"\x01" + pack[start:start + 65515])
        out += PKT_FLUSH
    else:
        out += pack
    return 200, "application/x-git-upload-pack-result", bytes(out)


class Backend:
    """Source of repositories for the HTTP application."""

    def open_repository(self, path):
        raise NotImplementedError


class UploadPackHandler:
    def __init__(self, backend, path):
        self.backend = backend
        self.path = path
        self.repo = None

    def handle(self, body):
        self.repo = self.backend.open_repository(self.path)
        return serve_upload_pack(self.repo, body)


class ReceivePackHandler:
    """git-receive-pack: unpack, then apply each ref update.

    Subclasses override _on_update(ref, old, new) to veto an update
    with a reason (bytes) — GitHub's fast-forward-only rule lives
    there in the forge."""

    def __init__(self, backend, path):
        self.backend = backend
        self.path = path
        self.repo = None

    def _on_update(self, ref_name, old_sha, new_sha):
        return None

    def handle(self, body):
        repo = self.backend.open_repository(self.path)
        self.repo = repo
        reader = PktReader(body)
        commands = []
        caps = set()
        first = True
        while True:
            packet = reader.read_packet()
            if packet is None or packet == b"":
                break
            line = packet[:-1] if packet.endswith(b"\n") else packet
            if first and b"\0" in line:
                line, _nul, captext = line.partition(b"\0")
                caps.update(p for p in captext.split(b" ") if p)
            first = False
            parts = line.split(b" ")
            if len(parts) != 3 or not _is_hex40(parts[0]) \
                    or not _is_hex40(parts[1]):
                raise ValueError("malformed ref update command")
            commands.append((parts[0], parts[1], parts[2]))
        pack_data = reader.rest()
        unpack_ok = True
        unpack_detail = b"ok"
        if pack_data:
            try:
                unpack_pack(pack_data, repo.object_store)
            except Exception as exc:
                unpack_ok = False
                unpack_detail = str(exc).encode(
                    "utf-8", "replace").splitlines()[0][:120] or b"error"
        results = []
        for old, new, ref in commands:
            if not unpack_ok:
                results.append((ref, False, b"unpacker error"))
                continue
            reason = self._apply(repo, old, new, ref, caps)
            results.append((ref, reason is None, reason or b""))
        if b"report-status" not in caps:
            return 200, "application/x-git-receive-pack-result", \
                PKT_FLUSH if b"side-band-64k" in caps else b""
        lines = [b"unpack %s\n" % unpack_detail]
        for ref, ok, reason in results:
            if ok:
                lines.append(b"ok %s\n" % ref)
            else:
                lines.append(b"ng %s %s\n" % (ref, reason))
        report = b"".join(pkt_line(line) for line in lines) + PKT_FLUSH
        if b"side-band-64k" in caps:
            out = bytearray()
            for start in range(0, len(report), 65515):
                out += pkt_line(b"\x01" + report[start:start + 65515])
            out += PKT_FLUSH
            report = bytes(out)
        return 200, "application/x-git-receive-pack-result", report

    def _apply(self, repo, old, new, ref, caps):
        """Apply one command; return None on success or the refusal
        reason (bytes) for the report."""
        refs = repo.refs
        store = repo.object_store
        if not ref.startswith(b"refs/") or b".." in ref \
                or b" " in ref or ref.endswith(b"/"):
            return b"invalid ref name"
        current = refs.read_ref(ref) or ZERO_SHA
        if new == ZERO_SHA:  # delete
            # delete-refs is a server capability (we advertise it),
            # not something the client requests per command.
            if ref == refs.get_symref(b"HEAD"):
                return b"deletion of the current branch prohibited"
            if current != old:
                return b"stale info"
            veto = self._on_update(ref, old, new)
            if veto:
                return veto
            refs.remove_ref(ref)
            return None
        if new not in store:
            return b"invalid object"
        if current != old:
            return b"stale info"
        veto = self._on_update(ref, old, new)
        if veto:
            return veto
        refs[ref] = new
        return None


class HTTPGitApplication:
    """WSGI smart-HTTP application: routes /<repo>.git/info/refs,
    /git-upload-pack, /git-receive-pack and /HEAD for any repository
    the backend opens."""

    def __init__(self, backend, handlers=None):
        self.backend = backend
        self.handlers = {
            b"git-upload-pack": UploadPackHandler,
            b"git-receive-pack": ReceivePackHandler,
        }
        if handlers:
            self.handlers.update(handlers)

    def __call__(self, environ, start_response):
        try:
            status, ctype, body = self._dispatch(environ)
        except Exception:
            import sys
            import traceback
            traceback.print_exc(file=environ.get("wsgi.errors")
                                or sys.stderr)
            status, ctype, body = 500, "text/plain", \
                b"internal server error\n"
        reason = {200: "OK", 400: "Bad Request",
                  404: "Not Found", 405: "Method Not Allowed",
                  500: "Internal Server Error"}.get(status, "OK")
        start_response("%d %s" % (status, reason), [
            ("Content-Type", ctype),
            ("Content-Length", str(len(body))),
        ])
        return [body]

    def _open(self, repo_path):
        return self.backend.open_repository(repo_path)

    def _dispatch(self, environ):
        from urllib.parse import parse_qs
        path = environ.get("PATH_INFO", "")
        method = environ.get("REQUEST_METHOD", "GET")
        if path.endswith("/info/refs"):
            if method != "GET":
                return 405, "text/plain", b"method not allowed\n"
            repo_path = path[:-len("/info/refs")]
            query = parse_qs(environ.get("QUERY_STRING", ""))
            service = query.get("service", [None])[0]
            try:
                repo = self._open(repo_path)
            except Exception:
                return 404, "text/plain", b"repository not found\n"
            if service is None:
                return 200, "text/plain; charset=utf-8", \
                    _dumb_refs(repo)
            if service not in ("git-upload-pack", "git-receive-pack"):
                return 400, "text/plain", b"unknown service\n"
            body = pkt_line(("# service=%s\n" % service).encode()) \
                + PKT_FLUSH + advertise_refs(repo, service)
            return 200, "application/x-%s-advertisement" % service, \
                body
        for suffix, service in (("/git-upload-pack",
                                 b"git-upload-pack"),
                                ("/git-receive-pack",
                                 b"git-receive-pack")):
            if path.endswith(suffix):
                if method != "POST":
                    return 405, "text/plain", b"method not allowed\n"
                repo_path = path[:-len(suffix)]
                length = int(environ.get("CONTENT_LENGTH") or 0)
                body = environ["wsgi.input"].read(length) \
                    if length else b""
                handler = self.handlers[service](self.backend,
                                                 repo_path)
                return handler.handle(body)
        if path.endswith("/HEAD"):
            if method != "GET":
                return 405, "text/plain", b"method not allowed\n"
            repo_path = path[:-len("/HEAD")]
            try:
                repo = self._open(repo_path)
            except Exception:
                return 404, "text/plain", b"repository not found\n"
            head_file = os.path.join(repo.path, "HEAD")
            try:
                with open(head_file, "rb") as fh:
                    content = fh.read()
            except OSError:
                return 404, "text/plain", b"no HEAD\n"
            return 200, "text/plain; charset=utf-8", content
        return 404, "text/plain", b"not found\n"

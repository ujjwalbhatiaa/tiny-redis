#!/usr/bin/env python3
"""tiny-redis: a tiny Redis clone in pure Python.

Speaks the RESP protocol, so `redis-cli` can talk to it. Supports strings,
lists, sets, hashes, sorted sets, key expiry, glob KEYS, and RDB-style
snapshots.
Standard library only.

    python tiny_redis.py --port 6379

"""

from __future__ import annotations

import argparse
import fnmatch
import math
import pickle
import socketserver
import threading
import time
from typing import Any

# ---------------------------------------------------------------------------
# RESP protocol
# ---------------------------------------------------------------------------


class ProtocolError(Exception):
    pass


class RespReader:
    """Incremental RESP parser over a socket."""

    def __init__(self, conn):
        self.conn = conn
        self.buf = b""

    def _fill(self, need: int = 1) -> None:
        while len(self.buf) < need:
            chunk = self.conn.recv(65536)
            if not chunk:
                raise ConnectionError("client closed connection")
            self.buf += chunk

    def _readline(self) -> bytes:
        while b"\r\n" not in self.buf:
            self._fill(len(self.buf) + 1)
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read_command(self) -> list[bytes]:
        line = self._readline()
        if not line.startswith(b"*"):
            raise ProtocolError("expected array")
        try:
            count = int(line[1:])
        except ValueError:
            raise ProtocolError("bad array length")
        if count < 0:
            raise ProtocolError("negative array length")
        args = []
        for _ in range(count):
            bulk = self._readline()
            if not bulk.startswith(b"$"):
                raise ProtocolError("expected bulk string")
            length = int(bulk[1:])
            if length < 0:
                args.append(None)
                continue
            self._fill(length + 2)
            args.append(self.buf[:length])
            self.buf = self.buf[length + 2:]
        return args


def encode(value: Any) -> bytes:
    """Serialize a Python value to RESP."""
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, str):
        if value.startswith("__err__"):
            return b"-" + value[7:].encode() + b"\r\n"
        if value.startswith("__ok__"):
            return b"+" + value[6:].encode() + b"\r\n"
        raw = value.encode()
        return b"$%d\r\n%s\r\n" % (len(raw), raw)
    if isinstance(value, bool):
        return b":%d\r\n" % int(value)
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, (list, tuple)):
        out = b"*%d\r\n" % len(value)
        for item in value:
            out += encode(item)
        return out
    raise TypeError(f"cannot encode {type(value)}")


def err(msg: str) -> str:
    return "__err__" + msg


def ok(msg: str = "OK") -> str:
    return "__ok__" + msg


WRONGTYPE = err("WRONGTYPE Operation against a key holding the wrong kind of value")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class Database:
    """Thread-safe keyspace with strings, lists, sets, hashes, sorted sets, and expiry."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[str, Any, float | None]] = {}
        self._lock = threading.RLock()

    # -- internals ---------------------------------------------------------

    def _get(self, key: str) -> tuple[str, Any] | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            typ, val, expire_at = entry
            if expire_at is not None and expire_at <= time.time():
                del self._data[key]
                return None
            return typ, val

    def _set(self, key: str, typ: str, val: Any,
             expire_at: float | None = None) -> None:
        with self._lock:
            self._data[key] = (typ, val, expire_at)

    def _typed(self, key: str, want: str) -> Any | None:
        """Return the value if the key exists with type `want`.

        Returns None for missing keys; raises WRONGTYPE (as _WrongType) for
        type mismatches — callers translate it to a RESP error.
        """
        found = self._get(key)
        if found is None:
            return None
        typ, val = found
        if typ != want:
            raise _WrongType()
        return val

    # -- persistence --------------------------------------------------------

    def save(self, path: str) -> None:
        with self._lock:
            now = time.time()
            live = {k: (t, v, e) for k, (t, v, e) in self._data.items()
                    if e is None or e > now}
            with open(path, "wb") as fh:
                pickle.dump(live, fh)

    def load(self, path: str) -> None:
        try:
            with open(path, "rb") as fh:
                data = pickle.load(fh)
        except (OSError, pickle.PickleError, EOFError):
            return
        now = time.time()
        with self._lock:
            self._data = {k: (t, v, e) for k, (t, v, e) in data.items()
                          if e is None or e > now}

    # -- command dispatch ----------------------------------------------------

    def execute(self, argv: list[bytes]) -> Any:
        if not argv:
            return err("ERR wrong number of arguments for '' command")
        cmd = argv[0].decode().upper()
        args = [a.decode() if a is not None else None for a in argv[1:]]
        handler = getattr(self, "cmd_" + cmd, None)
        if handler is None:
            return err(f"ERR unknown command '{argv[0].decode()}'")
        try:
            return handler(args)
        except _WrongType:
            return WRONGTYPE
        except _Arity as exc:
            return err(str(exc))

    # -- helpers --------------------------------------------------------------

    @staticmethod
    def _need(args: list, n: int, cmd: str) -> None:
        if len(args) < n:
            raise _Arity(f"ERR wrong number of arguments for '{cmd}' command")

    # -- connection -------------------------------------------------------------

    def cmd_PING(self, args):
        return ok("PONG") if not args else args[0]

    def cmd_ECHO(self, args):
        self._need(args, 1, "echo")
        return args[0]

    def cmd_QUIT(self, args):
        return ok()

    # -- strings ------------------------------------------------------------------

    def cmd_SET(self, args):
        self._need(args, 2, "set")
        key, val = args[0], args[1]
        expire_at = None
        nx = xx = False
        i = 2
        while i < len(args):
            opt = args[i].upper()
            if opt == "EX" and i + 1 < len(args):
                expire_at = time.time() + float(args[i + 1])
                i += 2
            elif opt == "PX" and i + 1 < len(args):
                expire_at = time.time() + float(args[i + 1]) / 1000
                i += 2
            elif opt == "NX":
                nx = True
                i += 1
            elif opt == "XX":
                xx = True
                i += 1
            else:
                return err("ERR syntax error")
        exists = self._get(key) is not None
        if nx and exists:
            return None
        if xx and not exists:
            return None
        self._set(key, "string", val, expire_at)
        return ok()

    def cmd_GET(self, args):
        self._need(args, 1, "get")
        val = self._typed(args[0], "string")
        return val  # None -> null bulk string

    def cmd_DEL(self, args):
        n = 0
        with self._lock:
            for k in args:
                if self._get(k) is not None:
                    del self._data[k]
                    n += 1
        return n

    def cmd_EXISTS(self, args):
        return sum(1 for k in args if self._get(k) is not None)

    def cmd_INCR(self, args):
        return self._incrby(args, 1, "incr")

    def cmd_DECR(self, args):
        return self._incrby(args, -1, "decr")

    def cmd_INCRBY(self, args):
        self._need(args, 2, "incrby")
        return self._incrby([args[0]], int(args[1]), "incrby")

    def _incrby(self, args, delta, cmd):
        self._need(args, 1, cmd)
        key = args[0]
        cur = self._typed(key, "string")
        val = 0 if cur is None else self._toint(cur, cmd)
        val += delta
        self._set(key, "string", str(val))
        return val

    @staticmethod
    def _toint(val, cmd):
        try:
            return int(val)
        except ValueError:
            raise _Arity("ERR value is not an integer or out of range")

    def cmd_INCRBYFLOAT(self, args):
        self._need(args, 2, "incrbyfloat")
        try:
            delta = float(args[1])
        except ValueError:
            raise _Arity("ERR value is not a valid float")
        if not math.isfinite(delta):
            raise _Arity("ERR value is not a valid float")
        key = args[0]
        cur = self._typed(key, "string")
        val = 0.0 if cur is None else self._tofloat(cur)
        new = val + delta
        if not math.isfinite(new):
            raise _Arity("ERR increment or decrement would overflow")
        out = repr(new)  # shortest round-trip string, like real Redis
        self._set(key, "string", out)
        return out

    @staticmethod
    def _tofloat(val):
        try:
            f = float(val)
        except ValueError:
            raise _Arity("ERR value is not a valid float")
        if not math.isfinite(f):
            raise _Arity("ERR value is not a valid float")
        return f

    def cmd_HINCRBY(self, args):
        self._need(args, 3, "hincrby")
        key, field = args[0], args[1]
        try:
            delta = int(args[2])
        except ValueError:
            raise _Arity("ERR value is not an integer or out of range")
        h = self._typed(key, "hash")  # WRONGTYPE on non-hash keys
        if h is None:
            h = {}
        self._set(key, "hash", h)
        cur = h.get(field)
        if cur is None:
            val = 0
        else:
            try:
                val = int(cur)
            except ValueError:
                raise _Arity("ERR hash value is not an integer")
        new = val + delta
        if not -(2 ** 63) <= new < 2 ** 63:
            raise _Arity("ERR increment or decrement would overflow")
        h[field] = str(new)
        return new

    def cmd_APPEND(self, args):
        self._need(args, 2, "append")
        cur = self._typed(args[0], "string") or ""
        new = cur + args[1]
        self._set(args[0], "string", new)
        return len(new)

    def cmd_STRLEN(self, args):
        self._need(args, 1, "strlen")
        val = self._typed(args[0], "string")
        return 0 if val is None else len(val)

    def cmd_MSET(self, args):
        self._need(args, 2, "mset")
        if len(args) % 2:
            raise _Arity("ERR wrong number of arguments for 'mset' command")
        with self._lock:  # one lock hold = the whole batch applies atomically
            for i in range(0, len(args), 2):
                self._set(args[i], "string", args[i + 1])
        return ok()

    def cmd_MGET(self, args):
        self._need(args, 1, "mget")
        # missing keys -> None (null bulk string); non-string keys -> WRONGTYPE
        return [self._typed(k, "string") for k in args]

    def cmd_SETNX(self, args):
        self._need(args, 2, "setnx")
        with self._lock:  # check-and-set must be atomic
            if self._get(args[0]) is not None:
                return 0
            self._set(args[0], "string", args[1])
        return 1

    def cmd_GETSET(self, args):
        self._need(args, 2, "getset")
        old = self._typed(args[0], "string")  # WRONGTYPE on non-string keys
        self._set(args[0], "string", args[1])  # like SET: clears any TTL
        return old  # None -> null bulk string when the key did not exist

    def cmd_MSETNX(self, args):
        self._need(args, 2, "msetnx")
        if len(args) % 2:
            raise _Arity("ERR wrong number of arguments for 'msetnx' command")
        with self._lock:  # one lock hold = check-then-set of the batch is atomic
            for i in range(0, len(args), 2):
                if self._get(args[i]) is not None:
                    return 0  # all-or-nothing: not a single key is set
            for i in range(0, len(args), 2):
                self._set(args[i], "string", args[i + 1])
        return 1

    # -- expiry -------------------------------------------------------------------

    def cmd_EXPIRE(self, args):
        self._need(args, 2, "expire")
        return self._expire(args[0], time.time() + float(args[1]))

    def cmd_PEXPIRE(self, args):
        self._need(args, 2, "pexpire")
        return self._expire(args[0], time.time() + float(args[1]) / 1000)

    def _expire(self, key, at):
        with self._lock:
            found = self._get(key)
            if found is None:
                return 0
            typ, val = found
            self._data[key] = (typ, val, at)
        return 1

    def cmd_TTL(self, args):
        self._need(args, 1, "ttl")
        return self._ttl(args[0], 1)

    def cmd_PTTL(self, args):
        self._need(args, 1, "pttl")
        return self._ttl(args[0], 1000)

    def _ttl(self, key, scale):
        with self._lock:
            entry = self._data.get(key)
            if entry is None or (entry[2] is not None
                                 and entry[2] <= time.time()):
                self._data.pop(key, None)
                return -2
            if entry[2] is None:
                return -1
            return int((entry[2] - time.time()) * scale)

    def cmd_PERSIST(self, args):
        self._need(args, 1, "persist")
        with self._lock:
            found = self._get(args[0])
            if found is None:
                return 0
            typ, val = found
            self._data[args[0]] = (typ, val, None)
        return 1

    def cmd_SETEX(self, args):
        self._need(args, 3, "setex")
        return self._setex(args[0], args[1], args[2], "setex", 1)

    def cmd_PSETEX(self, args):
        self._need(args, 3, "psetex")
        return self._setex(args[0], args[1], args[2], "psetex", 1000)

    def _setex(self, key, timeout, val, cmd, scale):
        # Like Redis: timeout must be a positive integer ("ERR invalid expire
        # time in '<cmd>' command" otherwise). SETEX/PSETEX overwrite any
        # existing key regardless of its type.
        try:
            ticks = int(timeout)
        except (ValueError, TypeError):
            return err(f"ERR invalid expire time in '{cmd}' command")
        if ticks <= 0:
            return err(f"ERR invalid expire time in '{cmd}' command")
        self._set(key, "string", val, time.time() + ticks / scale)
        return ok()

    # -- lists ----------------------------------------------------------------------

    def cmd_LPUSH(self, args):
        return self._push(args, "lpush", left=True)

    def cmd_RPUSH(self, args):
        return self._push(args, "rpush", left=False)

    def _push(self, args, cmd, left):
        self._need(args, 2, cmd)
        lst = self._typed(args[0], "list")
        if lst is None:
            lst = []
        self._set(args[0], "list", lst)
        for v in args[1:]:
            if left:
                lst.insert(0, v)
            else:
                lst.append(v)
        return len(lst)

    def cmd_LPUSHX(self, args):
        return self._pushx(args, "lpushx", left=True)

    def cmd_RPUSHX(self, args):
        return self._pushx(args, "rpushx", left=False)

    def _pushx(self, args, cmd, left):
        """LPUSHX/RPUSHX: push only when the key already exists as a list.

        Missing key (or expired): no-op returning 0, like real Redis.
        WRONGTYPE when the key exists but is not a list. Expiry is handled
        by _typed/_get, so an expired key behaves as missing.
        """
        self._need(args, 2, cmd)
        lst = self._typed(args[0], "list")
        if lst is None:
            return 0
        with self._lock:
            for v in args[1:]:
                if left:
                    lst.insert(0, v)
                else:
                    lst.append(v)
            return len(lst)

    def cmd_LPOP(self, args):
        return self._pop(args, "lpop", left=True)

    def cmd_RPOP(self, args):
        return self._pop(args, "rpop", left=False)

    def _pop(self, args, cmd, left):
        self._need(args, 1, cmd)
        lst = self._typed(args[0], "list")
        if not lst:
            return None
        return lst.pop(0) if left else lst.pop()

    def cmd_LRANGE(self, args):
        self._need(args, 3, "lrange")
        lst = self._typed(args[0], "list") or []
        start, stop = int(args[1]), int(args[2])
        n = len(lst)
        start = max(start + n if start < 0 else start, 0)
        stop = stop + n if stop < 0 else min(stop, n - 1)
        if start > stop or start >= n:
            return []
        return lst[start:stop + 1]

    def cmd_LLEN(self, args):
        self._need(args, 1, "llen")
        lst = self._typed(args[0], "list")
        return 0 if lst is None else len(lst)

    def cmd_LTRIM(self, args):
        self._need(args, 3, "ltrim")
        key, start_s, stop_s = args[0], args[1], args[2]
        with self._lock:
            found = self._data.get(key)
            if found is None:
                return ok()  # missing key: no-op, like real Redis
            typ, lst, expire_at = found
            if expire_at is not None and expire_at <= time.time():
                del self._data[key]
                return ok()  # expired key: treated as missing
            if typ != "list":
                raise _WrongType()
            n = len(lst)
            start = int(start_s)
            stop = int(stop_s)
            start = max(start + n if start < 0 else start, 0)
            stop = stop + n if stop < 0 else min(stop, n - 1)
            if start > stop or start >= n:
                del self._data[key]  # trimmed to empty: key removed
            else:
                self._data[key] = ("list", lst[start:stop + 1], expire_at)
        return ok()

    def cmd_LINDEX(self, args):
        self._need(args, 2, "lindex")
        lst = self._typed(args[0], "list")
        if lst is None:
            return None  # missing key: nil, like real Redis
        idx = int(args[1])
        if idx < 0:
            idx += len(lst)
        if idx < 0 or idx >= len(lst):
            return None  # out of range: nil
        return lst[idx]

    def cmd_LSET(self, args):
        self._need(args, 3, "lset")
        key = args[0]
        lst = self._typed(key, "list")
        if lst is None:
            return err("ERR no such key")
        idx = int(args[1])
        if idx < 0:
            idx += len(lst)
        if idx < 0 or idx >= len(lst):
            return err("ERR index out of range")
        with self._lock:
            lst[idx] = args[2]
        return ok()

    def cmd_LREM(self, args):
        self._need(args, 3, "lrem")
        key = args[0]
        count = int(args[1])
        element = args[2]
        lst = self._typed(key, "list")
        if lst is None:
            return 0  # missing key: nothing removed
        with self._lock:
            if count == 0:
                removed = sum(1 for v in lst if v == element)
                lst[:] = [v for v in lst if v != element]
            else:
                indices = range(len(lst)) if count > 0 else range(len(lst) - 1, -1, -1)
                to_delete = set()
                for i in indices:
                    if lst[i] == element:
                        to_delete.add(i)
                        if len(to_delete) == abs(count):
                            break
                removed = len(to_delete)
                lst[:] = [v for i, v in enumerate(lst) if i not in to_delete]
        return removed

    # -- sets -------------------------------------------------------------------------

    def cmd_SADD(self, args):
        self._need(args, 2, "sadd")
        s = self._typed(args[0], "set")
        if s is None:
            s = set()
        self._set(args[0], "set", s)
        added = 0
        for v in args[1:]:
            if v not in s:
                s.add(v)
                added += 1
        return added

    def cmd_SREM(self, args):
        self._need(args, 2, "srem")
        s = self._typed(args[0], "set")
        if s is None:
            return 0
        removed = sum(1 for v in args[1:] if v in s)
        s.difference_update(args[1:])
        return removed

    def cmd_SMEMBERS(self, args):
        self._need(args, 1, "smembers")
        s = self._typed(args[0], "set")
        return sorted(s) if s is not None else []

    def cmd_SISMEMBER(self, args):
        self._need(args, 2, "sismember")
        s = self._typed(args[0], "set")
        return 1 if s is not None and args[1] in s else 0

    def cmd_SCARD(self, args):
        self._need(args, 1, "scard")
        s = self._typed(args[0], "set")
        return 0 if s is None else len(s)

    # -- set algebra --------------------------------------------------------------

    def cmd_SINTER(self, args):
        self._need(args, 1, "sinter")
        return sorted(set.intersection(*self._set_operands(args)))

    def cmd_SUNION(self, args):
        self._need(args, 1, "sunion")
        return sorted(set.union(*self._set_operands(args)))

    def cmd_SDIFF(self, args):
        self._need(args, 1, "sdiff")
        return sorted(set.difference(*self._set_operands(args)))

    def _set_operands(self, args):
        """Member sets for one key each; missing keys count as empty sets.

        Non-set keys raise _WrongType (surfaced as WRONGTYPE by the
        dispatcher), so set algebra never silently mixes types.
        """
        return [self._typed(k, "set") or set() for k in args]

    # -- hashes -------------------------------------------------------------------------

    def cmd_HSET(self, args):
        self._need(args, 3, "hset")
        if (len(args) - 1) % 2:
            return err("ERR wrong number of arguments for 'hset' command")
        h = self._typed(args[0], "hash")
        if h is None:
            h = {}
        self._set(args[0], "hash", h)
        added = 0
        it = iter(args[1:])
        for f, v in zip(it, it):
            if f not in h:
                added += 1
            h[f] = v
        return added

    def cmd_HGET(self, args):
        self._need(args, 2, "hget")
        h = self._typed(args[0], "hash")
        return h.get(args[1]) if h is not None else None

    def cmd_HDEL(self, args):
        self._need(args, 2, "hdel")
        h = self._typed(args[0], "hash")
        if h is None:
            return 0
        n = sum(1 for f in args[1:] if f in h)
        for f in args[1:]:
            h.pop(f, None)
        return n

    def cmd_HGETALL(self, args):
        self._need(args, 1, "hgetall")
        h = self._typed(args[0], "hash")
        if not h:
            return []
        return [x for pair in sorted(h.items()) for x in pair]

    def cmd_HEXISTS(self, args):
        self._need(args, 2, "hexists")
        h = self._typed(args[0], "hash")
        return 1 if h is not None and args[1] in h else 0

    def cmd_HLEN(self, args):
        self._need(args, 1, "hlen")
        h = self._typed(args[0], "hash")
        return 0 if h is None else len(h)

    def cmd_HSETNX(self, args):
        """Set a hash field only if it does not already exist.

        Creates the key when missing; returns 1 when the field was set,
        0 when the field already existed (value untouched). Same key
        handling as HSET (WRONGTYPE on non-hash keys).
        """
        self._need(args, 3, "hsetnx")
        h = self._typed(args[0], "hash")
        if h is None:
            h = {}
        self._set(args[0], "hash", h)
        if args[1] in h:
            return 0
        h[args[1]] = args[2]
        return 1

    def _hash_fields_sorted(self, key):
        """(fields, values) for a hash, sorted by field like HGETALL."""
        h = self._typed(key, "hash")
        if not h:
            return [], []
        fields = sorted(h)
        return fields, [h[f] for f in fields]

    def cmd_HKEYS(self, args):
        self._need(args, 1, "hkeys")
        fields, _ = self._hash_fields_sorted(args[0])
        return fields

    def cmd_HVALS(self, args):
        self._need(args, 1, "hvals")
        _, values = self._hash_fields_sorted(args[0])
        return values

    # -- sorted sets --------------------------------------------------------------------

    def _zset_ordered(self, z: dict) -> list[str]:
        """Members ordered by (score, member), like real Redis."""
        return sorted(z, key=lambda m: (z[m], m))

    def cmd_ZADD(self, args):
        self._need(args, 3, "zadd")
        if len(args) % 2 == 0:
            raise _Arity("ERR wrong number of arguments for 'zadd' command")
        key = args[0]
        pairs = []
        it = iter(args[1:])
        for score_s, member in zip(it, it):
            try:
                score = float(score_s)
            except ValueError:
                raise _Arity("ERR value is not a valid float")
            if not math.isfinite(score):
                raise _Arity("ERR value is not a valid float")
            pairs.append((member, score))
        with self._lock:  # one lock hold = the whole batch applies atomically
            z = self._typed(key, "zset")  # WRONGTYPE on non-zset keys
            expire_at = self._data[key][2] if key in self._data else None
            z = dict(z) if z is not None else {}
            added = 0
            for member, score in pairs:
                if member not in z:
                    added += 1
                z[member] = score
            self._data[key] = ("zset", z, expire_at)
        return added

    def cmd_ZSCORE(self, args):
        self._need(args, 2, "zscore")
        z = self._typed(args[0], "zset")
        if z is None:
            return None
        score = z.get(args[1])
        # shortest round-trip string, like INCRBYFLOAT above
        return None if score is None else repr(score)

    def cmd_ZRANGE(self, args):
        self._need(args, 3, "zrange")
        z = self._typed(args[0], "zset") or {}
        try:
            start, stop = int(args[1]), int(args[2])
        except ValueError:
            raise _Arity("ERR value is not an integer or out of range")
        ordered = self._zset_ordered(z)
        n = len(ordered)
        start = max(start + n if start < 0 else start, 0)
        stop = stop + n if stop < 0 else min(stop, n - 1)
        if start > stop or start >= n:
            return []
        return ordered[start:stop + 1]

    def cmd_ZCARD(self, args):
        self._need(args, 1, "zcard")
        z = self._typed(args[0], "zset")
        return 0 if z is None else len(z)

    def cmd_ZREM(self, args):
        self._need(args, 2, "zrem")
        z = self._typed(args[0], "zset")
        if z is None:
            return 0
        with self._lock:
            removed = sum(1 for m in args[1:] if m in z)
            for m in args[1:]:
                z.pop(m, None)
        return removed

    def cmd_ZRANK(self, args):
        self._need(args, 2, "zrank")
        z = self._typed(args[0], "zset")
        if z is None or args[1] not in z:
            return None
        return self._zset_ordered(z).index(args[1])

    def cmd_ZINCRBY(self, args):
        self._need(args, 3, "zincrby")
        try:
            increment = float(args[1])
        except ValueError:
            raise _Arity("ERR value is not a valid float")
        key, member = args[0], args[2]
        with self._lock:
            z = self._typed(key, "zset")  # WRONGTYPE on non-zset keys
            expire_at = self._data[key][2] if key in self._data else None
            z = dict(z) if z is not None else {}
            new_score = z.get(member, 0.0) + increment
            if not math.isfinite(new_score):
                raise _Arity("ERR increment or decrement would result in NaN or Infinity")
            z[member] = new_score
            self._data[key] = ("zset", z, expire_at)
        # shortest round-trip string, like ZSCORE
        return repr(new_score)

    @staticmethod
    def _zcount_bound(raw):
        """Parse a ZCOUNT min/max bound: -inf/+inf, floats, '(x' exclusive."""
        exclusive = raw.startswith("(")
        text = raw[1:] if exclusive else raw
        if text == "-inf":
            return float("-inf"), exclusive
        if text == "+inf":
            return float("inf"), exclusive
        try:
            return float(text), exclusive
        except ValueError:
            raise _Arity("ERR min or max is not a float")

    def cmd_ZCOUNT(self, args):
        self._need(args, 3, "zcount")
        lo, lo_excl = self._zcount_bound(args[1])
        hi, hi_excl = self._zcount_bound(args[2])
        z = self._typed(args[0], "zset") or {}
        count = 0
        for score in z.values():
            above_lo = score > lo or (score == lo and not lo_excl)
            below_hi = score < hi or (score == hi and not hi_excl)
            if above_lo and below_hi:
                count += 1
        return count

    def cmd_ZREVRANGE(self, args):
        self._need(args, 3, "zrevrange")
        withscores = len(args) == 4 and args[3].upper() == "WITHSCORES"
        if len(args) > 4 or (len(args) == 4 and not withscores):
            raise _Arity("ERR syntax error")
        z = self._typed(args[0], "zset") or {}
        try:
            start, stop = int(args[1]), int(args[2])
        except ValueError:
            raise _Arity("ERR value is not an integer or out of range")
        ordered = list(reversed(self._zset_ordered(z)))
        n = len(ordered)
        start = max(start + n if start < 0 else start, 0)
        stop = stop + n if stop < 0 else min(stop, n - 1)
        if start > stop or start >= n:
            return []
        members = ordered[start:stop + 1]
        if not withscores:
            return members
        out = []
        for m in members:
            out.extend([m, repr(z[m])])
        return out

    # -- keys -----------------------------------------------------------------------------

    def cmd_KEYS(self, args):
        self._need(args, 1, "keys")
        with self._lock:
            now = time.time()
            return sorted(k for k, (_, _, exp) in self._data.items()
                          if (exp is None or exp > now)
                          and fnmatch.fnmatch(k, args[0]))

    def cmd_SCAN(self, args):
        self._need(args, 1, "scan")
        try:
            cursor = int(args[0])
        except ValueError:
            raise _Arity("ERR value is not an integer or out of range")
        if cursor < 0:
            raise _Arity("ERR invalid cursor")
        pattern, count = "*", 10
        i = 1
        while i < len(args):
            opt = args[i].upper()
            if opt == "MATCH" and i + 1 < len(args):
                pattern = args[i + 1]
                i += 2
            elif opt == "COUNT" and i + 1 < len(args):
                try:
                    count = int(args[i + 1])
                except ValueError:
                    raise _Arity("ERR value is not an integer or out of range")
                if count < 1:
                    raise _Arity("ERR syntax error")
                i += 2
            else:
                return err("ERR syntax error")
        # A fresh snapshot per call (sorted, so deletions between calls only
        # shrink the list instead of shifting positions unpredictably).
        # Cursor is a plain integer offset; the server returns cursor "0"
        # when the iteration is complete. Like real Redis, keys added or
        # removed mid-iteration may be visited twice or skipped — the cursor
        # is a convenience for incremental consumption, not a transaction.
        with self._lock:
            now = time.time()
            keys = sorted(k for k, (_, _, exp) in self._data.items()
                          if (exp is None or exp > now)
                          and fnmatch.fnmatch(k, pattern))
            page = keys[cursor:cursor + count]
            nxt = cursor + count
            return [str(0 if nxt >= len(keys) else nxt), page]

    def cmd_TYPE(self, args):
        self._need(args, 1, "type")
        found = self._get(args[0])
        return ok("none") if found is None else ok(found[0])

    def cmd_FLUSHDB(self, args):
        with self._lock:
            self._data.clear()
        return ok()

    def cmd_DBSIZE(self, args):
        with self._lock:
            now = time.time()
            return sum(1 for (_, _, exp) in self._data.values()
                       if exp is None or exp > now)

    # -- server -----------------------------------------------------------------------------

    def cmd_INFO(self, args):
        with self._lock:
            n = len(self._data)
        return (f"tiny_redis_version:0.1.0\r\nkeys:{n}\r\n"
                f"uptime_seconds:{int(time.time() - _STARTED_AT)}\r\n")

    def cmd_SAVE(self, args):
        self.save(_DB_PATH[0])
        return ok()

    def cmd_COMMAND(self, args):
        return []


class _WrongType(Exception):
    pass


class _Arity(Exception):
    pass


_STARTED_AT = time.time()

_DB_PATH = ["dump.rdb"]

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        reader = RespReader(self.request)
        db: Database = self.server.db
        while True:
            try:
                argv = reader.read_command()
            except (ConnectionError, ProtocolError):
                return
            if not argv:
                continue
            result = db.execute(argv)
            try:
                self.request.sendall(encode(result))
            except OSError:
                return
            if argv and argv[0].upper() == b"QUIT":
                return


class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve(port: int = 6379, db_path: str = "dump.rdb") -> ThreadedServer:
    db = Database()
    db.load(db_path)
    _DB_PATH[0] = db_path
    server = ThreadedServer(("127.0.0.1", port), _Handler)
    server.db = db
    return server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="tiny-redis server")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--dbfile", default="dump.rdb")
    args = parser.parse_args(argv)
    server = serve(args.port, args.dbfile)
    print(f"tiny-redis listening on 127.0.0.1:{args.port} "
          f"(dbfile: {args.dbfile})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

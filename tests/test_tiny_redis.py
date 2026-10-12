"""Tests for tiny-redis. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it.
"""

import socket
import threading
import time

import pytest

from tiny_redis import ThreadedServer, Database, _Handler, encode


@pytest.fixture()
def client():
    server = ThreadedServer(("127.0.0.1", 0), _Handler)
    server.db = Database()
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    buf = b""

    def cmd(*args):
        nonlocal buf
        out = b"*%d\r\n" % len(args)
        for a in args:
            raw = a.encode() if isinstance(a, str) else a
            out += b"$%d\r\n%s\r\n" % (len(raw), raw)
        sock.sendall(out)
        return _read_reply()

    def _read_reply():
        nonlocal buf
        def readline():
            nonlocal buf
            while b"\r\n" not in buf:
                chunk = sock.recv(65536)
                assert chunk, "server closed connection"
                buf += chunk
            line, buf = buf.split(b"\r\n", 1)
            return line
        line = readline()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return ("ERROR", rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(buf) < n + 2:
                buf += sock.recv(65536)
            data, buf = buf[:n], buf[n + 2:]
            return data.decode()
        if kind == b"*":
            return [_read_reply() for _ in range(int(rest))]
        raise AssertionError(f"unknown reply type {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()
    server.server_close()


def test_ping(client):
    assert client("PING") == "PONG"
    assert client("PING", "hello") == "hello"


def test_echo(client):
    assert client("ECHO", "hi") == "hi"


def test_set_get(client):
    assert client("SET", "k", "v") == "OK"
    assert client("GET", "k") == "v"
    assert client("GET", "missing") is None


def test_set_nx_xx(client):
    assert client("SET", "k", "1", "NX") == "OK"
    assert client("SET", "k", "2", "NX") is None
    assert client("GET", "k") == "1"
    assert client("SET", "k", "3", "XX") == "OK"
    assert client("SET", "nope", "x", "XX") is None


def test_del_exists(client):
    client("SET", "a", "1")
    client("SET", "b", "2")
    assert client("EXISTS", "a", "b", "zzz") == 2
    assert client("DEL", "a", "zzz") == 1
    assert client("EXISTS", "a") == 0


def test_incr_decr(client):
    assert client("INCR", "n") == 1
    assert client("INCR", "n") == 2
    assert client("INCRBY", "n", "8") == 10
    assert client("DECR", "n") == 9
    client("SET", "s", "notanint")
    kind, _ = client("INCR", "s")
    assert kind == "ERROR"


def test_append_strlen(client):
    assert client("APPEND", "k", "foo") == 3
    assert client("APPEND", "k", "bar") == 6
    assert client("STRLEN", "k") == 6
    assert client("STRLEN", "missing") == 0


def test_mset_mget(client):
    assert client("MSET", "a", "1", "b", "2", "c", "3") == "OK"
    assert client("MGET", "a", "b", "c") == ["1", "2", "3"]
    # missing keys come back as nulls, in order
    assert client("MGET", "a", "zzz", "b") == ["1", None, "2"]
    # MSET overwrites existing keys, including non-string ones
    client("RPUSH", "l", "x")
    assert client("MSET", "l", "now-a-string") == "OK"
    assert client("GET", "l") == "now-a-string"
    # arity errors: odd arg count or fewer than 2 args
    kind, _ = client("MSET", "onlykey")
    assert kind == "ERROR"
    kind, _ = client("MSET", "a", "1", "b")
    assert kind == "ERROR"
    kind, _ = client("MGET")
    assert kind == "ERROR"


def test_mget_wrongtype(client):
    client("RPUSH", "l", "x")
    kind, _ = client("MGET", "l")
    assert kind == "ERROR"


def test_setnx(client):
    # new key -> set, returns 1
    assert client("SETNX", "k", "v") == 1
    assert client("GET", "k") == "v"
    # existing key -> no-op, returns 0, value untouched
    assert client("SETNX", "k", "other") == 0
    assert client("GET", "k") == "v"
    # existing non-string key -> no-op, returns 0 (never overwrites the type)
    client("RPUSH", "l", "x")
    assert client("SETNX", "l", "y") == 0
    assert client("TYPE", "l") == "list"
    # arity
    kind, _ = client("SETNX", "onlykey")
    assert kind == "ERROR"


def test_getset(client):
    # new key -> nil, and the value is stored
    assert client("GETSET", "k", "v1") is None
    assert client("GET", "k") == "v1"
    # existing key -> old value returned, new value stored
    assert client("GETSET", "k", "v2") == "v1"
    assert client("GET", "k") == "v2"
    # non-string key -> WRONGTYPE, value untouched
    client("RPUSH", "l", "x")
    kind, _ = client("GETSET", "l", "y")
    assert kind == "ERROR"
    assert client("TYPE", "l") == "list"
    # arity
    kind, _ = client("GETSET", "onlykey")
    assert kind == "ERROR"


def test_msetnx(client):
    # all keys new -> 1, everything set
    assert client("MSETNX", "a", "1", "b", "2") == 1
    assert client("MGET", "a", "b") == ["1", "2"]
    # one key exists -> 0 and NOTHING is set (all-or-nothing)
    assert client("MSETNX", "b", "X", "c", "3") == 0
    assert client("GET", "b") == "2"
    assert client("GET", "c") is None
    # an existing non-string key blocks the whole batch too
    client("RPUSH", "l", "x")
    assert client("MSETNX", "d", "4", "l", "y") == 0
    assert client("GET", "d") is None
    assert client("TYPE", "l") == "list"
    # arity errors: odd arg count or fewer than 2 args
    kind, _ = client("MSETNX", "onlykey")
    assert kind == "ERROR"
    kind, _ = client("MSETNX", "a", "1", "b")
    assert kind == "ERROR"
    kind, _ = client("MSETNX")
    assert kind == "ERROR"


def test_list_commands(client):
    assert client("RPUSH", "l", "a", "b", "c") == 3
    assert client("LPUSH", "l", "z") == 4
    assert client("LRANGE", "l", "0", "-1") == ["z", "a", "b", "c"]
    assert client("LRANGE", "l", "1", "2") == ["a", "b"]
    assert client("LLEN", "l") == 4
    assert client("LPOP", "l") == "z"
    assert client("RPOP", "l") == "c"
    assert client("LPOP", "missing") is None


def test_set_commands(client):
    assert client("SADD", "s", "a", "b", "a") == 2
    assert client("SCARD", "s") == 2
    assert client("SISMEMBER", "s", "a") == 1
    assert client("SISMEMBER", "s", "zzz") == 0
    assert client("SMEMBERS", "s") == ["a", "b"]
    assert client("SREM", "s", "a", "zzz") == 1
    assert client("SMEMBERS", "s") == ["b"]


def test_set_algebra(client):
    client("SADD", "a", "1", "2", "3", "4")
    client("SADD", "b", "3", "4", "5")
    client("SADD", "c", "4")
    # intersection, union, difference — results sorted like SMEMBERS
    assert client("SINTER", "a", "b") == ["3", "4"]
    assert client("SINTER", "a", "b", "c") == ["4"]
    assert client("SUNION", "a", "b") == ["1", "2", "3", "4", "5"]
    assert client("SDIFF", "a", "b") == ["1", "2"]
    assert client("SDIFF", "b", "a") == ["5"]
    # missing keys behave as empty sets
    assert client("SINTER", "a", "nope") == []
    assert client("SUNION", "nope", "nada") == []
    assert client("SDIFF", "a", "nope") == ["1", "2", "3", "4"]
    assert client("SDIFF", "nope", "a") == []
    # source sets are never mutated by the operations
    assert client("SMEMBERS", "a") == ["1", "2", "3", "4"]
    # WRONGTYPE on non-set operands, arity on zero keys
    client("SET", "s", "x")
    kind, _ = client("SINTER", "a", "s")
    assert kind == "ERROR"
    kind, _ = client("SDIFF", "s", "a")
    assert kind == "ERROR"
    kind, _ = client("SUNION")
    assert kind == "ERROR"
    kind, _ = client("SINTER")
    assert kind == "ERROR"


def test_hash_commands(client):
    assert client("HSET", "h", "f1", "v1", "f2", "v2") == 2
    assert client("HSET", "h", "f1", "v1b") == 0
    assert client("HGET", "h", "f1") == "v1b"
    assert client("HGET", "h", "nope") is None
    assert client("HEXISTS", "h", "f2") == 1
    assert client("HLEN", "h") == 2
    assert client("HGETALL", "h") == ["f1", "v1b", "f2", "v2"]
    assert client("HDEL", "h", "f1", "zzz") == 1
    assert client("HLEN", "h") == 1


def test_wrongtype(client):
    client("SET", "str", "x")
    kind, _ = client("LPUSH", "str", "y")
    assert kind == "ERROR"
    kind, _ = client("HGET", "str", "f")
    assert kind == "ERROR"


def test_expire_ttl(client):
    client("SET", "k", "v")
    assert client("TTL", "k") == -1
    assert client("EXPIRE", "k", "100") == 1
    ttl = client("TTL", "k")
    assert 0 < ttl <= 100
    assert client("PERSIST", "k") == 1
    assert client("TTL", "k") == -1
    assert client("TTL", "missing") == -2
    assert client("EXPIRE", "missing", "10") == 0


def test_expiry_actually_expires(client):
    client("SET", "k", "v", "PX", "80")
    assert client("GET", "k") == "v"
    time.sleep(0.15)
    assert client("GET", "k") is None
    assert client("TTL", "k") == -2


def test_keys_glob(client):
    client("SET", "user:1", "a")
    client("SET", "user:2", "b")
    client("SET", "other", "c")
    assert client("KEYS", "user:*") == ["user:1", "user:2"]
    assert client("KEYS", "*") == ["other", "user:1", "user:2"]


def test_type_and_dbsize(client):
    client("SET", "s", "x")
    client("RPUSH", "l", "x")
    assert client("TYPE", "s") == "string"
    assert client("TYPE", "l") == "list"
    assert client("TYPE", "nope") == "none"
    assert client("DBSIZE") == 2
    client("FLUSHDB")
    assert client("DBSIZE") == 0


def test_unknown_command(client):
    kind, msg = client("FROBNICATE")
    assert kind == "ERROR"
    assert "unknown command" in msg


def test_save_and_load(tmp_path):
    dbfile = str(tmp_path / "dump.rdb")
    db = Database()
    db.execute([b"SET", b"k", b"v"])
    db.execute([b"RPUSH", b"l", b"a"])
    db.execute([b"SADD", b"s", b"x"])
    db.save(dbfile)

    db2 = Database()
    db2.load(dbfile)
    assert db2.execute([b"GET", b"k"]) == "v"
    assert db2.execute([b"LRANGE", b"l", b"0", b"-1"]) == ["a"]
    assert db2.execute([b"SMEMBERS", b"s"]) == ["x"]


def test_save_skips_expired(tmp_path):
    dbfile = str(tmp_path / "dump.rdb")
    db = Database()
    db.execute([b"SET", b"gone", b"v", b"PX", b"1"])
    time.sleep(0.05)
    db.save(dbfile)
    db2 = Database()
    db2.load(dbfile)
    assert db2.execute([b"GET", b"gone"]) is None


def test_incrbyfloat(client):
    assert client("INCRBYFLOAT", "f", "2.5") == "2.5"
    assert client("INCRBYFLOAT", "f", "1.25") == "3.75"
    assert client("INCRBYFLOAT", "f", "-0.75") == "3.0"
    assert client("GET", "f") == "3.0"
    # missing key starts from 0
    assert client("INCRBYFLOAT", "fresh", "-1.5") == "-1.5"


def test_incrbyfloat_errors(client):
    client("SET", "s", "abc")
    kind, msg = client("INCRBYFLOAT", "s", "1.5")
    assert kind == "ERROR" and "not a valid float" in msg
    kind, msg = client("INCRBYFLOAT", "ok", "xyz")
    assert kind == "ERROR" and "not a valid float" in msg
    client("RPUSH", "l", "x")
    kind, msg = client("INCRBYFLOAT", "l", "1.5")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_hincrby(client):
    assert client("HINCRBY", "h", "f", "5") == 5
    assert client("HINCRBY", "h", "f", "-2") == 3
    assert client("HINCRBY", "h", "new", "10") == 10  # missing field starts at 0
    assert client("HGET", "h", "f") == "3"
    assert client("HGET", "h", "new") == "10"


def test_hincrby_errors(client):
    client("SET", "s", "x")
    kind, msg = client("HINCRBY", "s", "f", "1")
    assert kind == "ERROR" and "WRONGTYPE" in msg
    client("HSET", "h", "bad", "nope")
    kind, msg = client("HINCRBY", "h", "bad", "1")
    assert kind == "ERROR" and "not an integer" in msg
    kind, msg = client("HINCRBY", "h", "f", "1.5")
    assert kind == "ERROR" and "not an integer" in msg

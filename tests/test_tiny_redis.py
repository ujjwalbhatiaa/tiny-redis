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

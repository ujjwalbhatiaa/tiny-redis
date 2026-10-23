"""Tests for tiny-redis SETEX / PSETEX. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_lpushx_rpushx.py.
"""

import socket
import threading
import time

import pytest

from tiny_redis import ThreadedServer, Database, _Handler


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
        raise AssertionError("unexpected reply kind: %r" % kind)

    yield cmd
    sock.close()
    server.shutdown()


def test_setex_sets_value_and_ttl(client):
    assert client("SETEX", "k1", "100", "hello") == "OK"
    assert client("GET", "k1") == "hello"
    ttl = client("TTL", "k1")
    assert 0 < ttl <= 100


def test_psetex_sets_value_and_pttl(client):
    assert client("PSETEX", "k2", "60000", "world") == "OK"
    assert client("GET", "k2") == "world"
    pttl = client("PTTL", "k2")
    assert 0 < pttl <= 60000


def test_setex_overwrites_any_type(client):
    assert client("RPUSH", "lst", "a") == 1
    assert client("SETEX", "lst", "100", "now-a-string") == "OK"
    assert client("TYPE", "lst") == "string"
    assert client("GET", "lst") == "now-a-string"


def test_setex_overwrites_existing_string(client):
    assert client("SET", "k3", "old") == "OK"
    assert client("SETEX", "k3", "50", "new") == "OK"
    assert client("GET", "k3") == "new"
    ttl = client("TTL", "k3")
    assert 0 < ttl <= 50


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "1.5", ""])
def test_setex_rejects_bad_timeout(client, bad):
    kind, msg = client("SETEX", "kbad", bad, "v")
    assert kind == "ERROR"
    assert msg == "ERR invalid expire time in 'setex' command"
    assert client("GET", "kbad") is None


@pytest.mark.parametrize("bad", ["0", "-1", "xyz"])
def test_psetex_rejects_bad_timeout(client, bad):
    kind, msg = client("PSETEX", "kbad", bad, "v")
    assert kind == "ERROR"
    assert msg == "ERR invalid expire time in 'psetex' command"
    assert client("GET", "kbad") is None


def test_psetex_key_actually_expires(client):
    assert client("PSETEX", "ephem", "80", "gone-soon") == "OK"
    assert client("GET", "ephem") == "gone-soon"
    time.sleep(0.3)
    assert client("GET", "ephem") is None
    assert client("TTL", "ephem") == -2
    assert client("EXISTS", "ephem") == 0


def test_setex_persists_across_save_load(tmp_path):
    db = Database()
    db._set("sv", "string", "keepme", __import__("time").time() + 100)
    path = str(tmp_path / "dump.rdb")
    db.save(path)
    db2 = Database()
    db2.load(path)
    assert db2._get("sv") == ("string", "keepme")


def test_setex_wrong_arity(client):
    kind, msg = client("SETEX", "only-key")
    assert kind == "ERROR"
    assert "wrong number of arguments" in msg
    kind, msg = client("PSETEX", "k", "1000")
    assert kind == "ERROR"
    assert "wrong number of arguments" in msg

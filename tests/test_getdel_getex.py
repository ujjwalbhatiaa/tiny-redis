"""Tests for tiny-redis GETDEL / GETEX. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_setex_psetex.py.
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


def test_getdel_returns_value_and_deletes(client):
    assert client("SET", "k1", "hello") == "OK"
    assert client("GETDEL", "k1") == "hello"
    assert client("GET", "k1") is None
    assert client("EXISTS", "k1") == 0


def test_getdel_missing_key_returns_nil(client):
    assert client("GETDEL", "nope") is None


def test_getdel_wrong_type_errors_and_keeps_key(client):
    assert client("RPUSH", "lst", "a") == 1
    kind, msg = client("GETDEL", "lst")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg
    assert client("LLEN", "lst") == 1


def test_getex_no_option_returns_value_unchanged(client):
    assert client("SET", "k2", "v") == "OK"
    assert client("GETEX", "k2") == "v"
    assert client("TTL", "k2") == -1


def test_getex_ex_sets_ttl(client):
    assert client("SET", "k3", "v") == "OK"
    assert client("GETEX", "k3", "EX", "100") == "v"
    ttl = client("TTL", "k3")
    assert 0 < ttl <= 100


def test_getex_px_sets_pttl_and_key_expires(client):
    assert client("SET", "k4", "v") == "OK"
    assert client("GETEX", "k4", "PX", "80") == "v"
    assert 0 < client("PTTL", "k4") <= 80
    time.sleep(0.3)
    assert client("GET", "k4") is None


def test_getex_persist_clears_expiry(client):
    assert client("SETEX", "k5", "100", "v") == "OK"
    assert client("GETEX", "k5", "PERSIST") == "v"
    assert client("TTL", "k5") == -1


def test_getex_missing_key_returns_nil(client):
    assert client("GETEX", "nope") is None
    assert client("GETEX", "nope", "EX", "10") is None


def test_getex_wrong_type_errors(client):
    assert client("RPUSH", "lst2", "a") == 1
    kind, msg = client("GETEX", "lst2", "EX", "10")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


@pytest.mark.parametrize("bad", ["0", "-5", "abc"])
def test_getex_rejects_bad_timeout(client, bad):
    assert client("SET", "kb", "v") == "OK"
    kind, msg = client("GETEX", "kb", "EX", bad)
    assert kind == "ERROR"
    assert msg == "ERR invalid expire time in 'getex' command"
    assert client("GET", "kb") == "v"


def test_getex_syntax_error(client):
    assert client("SET", "ks", "v") == "OK"
    kind, msg = client("GETEX", "ks", "KEEPTTL")
    assert kind == "ERROR"
    assert msg == "ERR syntax error"

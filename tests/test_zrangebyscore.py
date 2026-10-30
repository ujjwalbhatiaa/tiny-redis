"""Tests for tiny-redis ZRANGEBYSCORE. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zincrby_zcount_zrevrange.py.
"""

import socket
import threading

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
        if kind == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [_read_reply() for _ in range(n)]
        raise AssertionError(f"unknown reply kind {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()


def _seed(client):
    assert client("ZADD", "lb", "10", "alice", "5", "bob",
                  "7.5", "carol", "5", "dave") == 4


def test_rangebyscore_inclusive_bounds(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "5", "7.5") == ["bob", "dave", "carol"]


def test_rangebyscore_exclusive_lower(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "(5", "10") == ["carol", "alice"]


def test_rangebyscore_inf_bounds(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "-inf", "+inf") == [
        "bob", "dave", "carol", "alice"]


def test_rangebyscore_missing_key_is_empty(client):
    assert client("ZRANGEBYSCORE", "nope", "-inf", "+inf") == []


def test_rangebyscore_withscores(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "5", "5", "WITHSCORES") == [
        "bob", "5.0", "dave", "5.0"]


def test_rangebyscore_limit(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "-inf", "+inf",
                  "LIMIT", "1", "2") == ["dave", "carol"]
    assert client("ZRANGEBYSCORE", "lb", "-inf", "+inf",
                  "LIMIT", "2", "-1") == ["carol", "alice"]


def test_rangebyscore_bad_bound_and_syntax(client):
    _seed(client)
    assert client("ZRANGEBYSCORE", "lb", "x", "10")[0] == "ERROR"
    assert client("ZRANGEBYSCORE", "lb", "-inf", "+inf", "BOGUS")[0] == "ERROR"


def test_rangebyscore_wrongtype(client):
    client("SET", "s", "v")
    assert client("ZRANGEBYSCORE", "s", "-inf", "+inf") == (
        "ERROR", "WRONGTYPE Operation against a key holding the wrong kind of value")

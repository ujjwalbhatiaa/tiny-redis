"""Tests for tiny-redis ZREVRANGEBYSCORE. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zrangebyscore.py.
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
                if not chunk:
                    raise ConnectionError("server closed connection")
                buf += chunk
            line, buf = buf.split(b"\r\n", 1)
            return line

        head = readline()
        kind, rest = head[:1], head[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return ("ERROR", rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            while len(buf) < n + 2:
                chunk = sock.recv(65536)
                if not chunk:
                    raise ConnectionError("server closed connection")
                buf += chunk
            data, buf = buf[:n], buf[n + 2:]
            return data.decode()
        if kind == b"*":
            return [_read_reply() for _ in range(int(rest))]
        raise ValueError(f"unexpected RESP type {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()
    server.server_close()


def _seed(client):
    assert client("ZADD", "lb", "10", "alice", "5", "bob",
                  "7.5", "carol", "5", "dave") == 4


def test_revrangebyscore_descending_order(client):
    _seed(client)
    assert client("ZREVRANGEBYSCORE", "lb", "+inf", "-inf") == [
        "alice", "carol", "dave", "bob"]


def test_revrangebyscore_bounds(client):
    _seed(client)
    assert client("ZREVRANGEBYSCORE", "lb", "7.5", "5") == [
        "carol", "dave", "bob"]
    assert client("ZREVRANGEBYSCORE", "lb", "(7.5", "5") == ["dave", "bob"]
    assert client("ZREVRANGEBYSCORE", "lb", "10", "(5") == ["alice", "carol"]


def test_revrangebyscore_missing_key_is_empty(client):
    assert client("ZREVRANGEBYSCORE", "nope", "+inf", "-inf") == []


def test_revrangebyscore_withscores_and_limit(client):
    _seed(client)
    assert client("ZREVRANGEBYSCORE", "lb", "+inf", "-inf",
                  "WITHSCORES") == [
        "alice", "10.0", "carol", "7.5", "dave", "5.0", "bob", "5.0"]
    assert client("ZREVRANGEBYSCORE", "lb", "+inf", "-inf",
                  "LIMIT", "1", "2") == ["carol", "dave"]
    assert client("ZREVRANGEBYSCORE", "lb", "+inf", "-inf",
                  "LIMIT", "2", "-1") == ["dave", "bob"]


def test_revrangebyscore_bad_bound_and_syntax(client):
    _seed(client)
    assert client("ZREVRANGEBYSCORE", "lb", "x", "5")[0] == "ERROR"
    assert client("ZREVRANGEBYSCORE", "lb", "+inf", "-inf", "BOGUS")[0] == "ERROR"


def test_revrangebyscore_wrongtype(client):
    client("SET", "s", "v")
    assert client("ZREVRANGEBYSCORE", "s", "+inf", "-inf") == (
        "ERROR", "WRONGTYPE Operation against a key holding the wrong kind of value")

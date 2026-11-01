"""Tests for tiny-redis ZPOPMIN/ZPOPMAX. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zrevrangebyscore.py.
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
            data, buf = buf[:n], buf[n + 2:]
            return data.decode()
        if kind == b"*":
            n = int(rest)
            if n == -1:
                return None
            return [_read_reply() for _ in range(n)]
        raise ValueError(f"unexpected reply kind {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()


def _seed(client):
    assert client("ZADD", "lb", "5", "bob", "7.5", "carol",
                 "5", "alice", "10", "dave") == 4


def test_zpopmin_default_one(client):
    _seed(client)
    # Ties on score 5 break by member name: alice before bob.
    assert client("ZPOPMIN", "lb") == ["alice", "5.0"]
    assert client("ZRANGE", "lb", "0", "-1") == ["bob", "carol", "dave"]


def test_zpopmax_with_count(client):
    _seed(client)
    assert client("ZPOPMAX", "lb", "2") == ["dave", "10.0", "carol", "7.5"]
    assert client("ZRANGE", "lb", "0", "-1") == ["alice", "bob"]


def test_zpopmin_count_overrun_empties_key(client):
    _seed(client)
    assert client("ZPOPMIN", "lb", "99") == [
        "alice", "5.0", "bob", "5.0", "carol", "7.5", "dave", "10.0"]
    assert client("EXISTS", "lb") == 0
    assert client("ZPOPMIN", "lb") == []


def test_zpop_missing_key_and_bad_count(client):
    assert client("ZPOPMAX", "nope") == []
    _seed(client)
    assert client("ZPOPMIN", "lb", "x")[0] == "ERROR"
    assert client("ZPOPMAX", "lb", "-1")[0] == "ERROR"
    assert client("ZPOPMIN", "lb", "1", "extra")[0] == "ERROR"
    # Failed pops changed nothing.
    assert client("ZCARD", "lb") == 4


def test_zpop_wrongtype(client):
    assert client("SET", "str", "v") == "OK"
    assert client("ZPOPMIN", "str")[0] == "ERROR"

"""Tests for tiny-redis ZREMRANGEBYRANK. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zremrangebyscore.py.
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
    # Score order: alice (5), bob (5), carol (7.5), dave (10).
    assert client("ZADD", "lb", "5", "bob", "7.5", "carol",
                 "5", "alice", "10", "dave") == 4


def test_zremrangebyrank_inclusive_range(client):
    _seed(client)
    # Ranks 1..2 are bob and carol.
    assert client("ZREMRANGEBYRANK", "lb", "1", "2") == 2
    assert client("ZRANGE", "lb", "0", "-1") == ["alice", "dave"]
    assert client("ZCARD", "lb") == 2


def test_zremrangebyrank_negative_indices(client):
    _seed(client)
    # -2..-1 are the two top members, carol and dave.
    assert client("ZREMRANGEBYRANK", "lb", "-2", "-1") == 2
    assert client("ZRANGE", "lb", "0", "-1") == ["alice", "bob"]


def test_zremrangebyrank_clamping_empties_key(client):
    _seed(client)
    # Out-of-range stop clamps to the last member: all four go.
    assert client("ZREMRANGEBYRANK", "lb", "0", "99") == 4
    assert client("EXISTS", "lb") == 0
    assert client("ZCARD", "lb") == 0


def test_zremrangebyrank_empty_range_and_missing_key(client):
    _seed(client)
    assert client("ZREMRANGEBYRANK", "lb", "3", "1") == 0
    assert client("ZCARD", "lb") == 4
    # Start past the end: nothing removed.
    assert client("ZREMRANGEBYRANK", "lb", "9", "12") == 0
    assert client("ZCARD", "lb") == 4
    assert client("ZREMRANGEBYRANK", "nope", "0", "10") == 0


def test_zremrangebyrank_errors_and_wrongtype(client):
    _seed(client)
    assert client("ZREMRANGEBYRANK", "lb", "x", "10")[0] == "ERROR"
    assert client("ZREMRANGEBYRANK", "lb", "0", "y")[0] == "ERROR"
    assert client("ZREMRANGEBYRANK", "lb", "0", "10", "extra")[0] == "ERROR"
    assert client("ZREMRANGEBYRANK", "lb")[0] == "ERROR"
    # Failed removals changed nothing.
    assert client("ZCARD", "lb") == 4
    assert client("SET", "str", "v") == "OK"
    assert client("ZREMRANGEBYRANK", "str", "0", "10")[0] == "ERROR"

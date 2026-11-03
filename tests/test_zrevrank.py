"""Tests for tiny-redis ZREVRANK. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zpop.py.
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
        raise ValueError(f"unexpected reply kind {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()


def _seed(client):
    assert client("ZADD", "lb", "5", "bob", "7.5", "carol",
                 "5", "alice", "10", "dave") == 4


def test_zrevrank_is_reverse_of_zrank(client):
    _seed(client)
    # Ascending order: alice(5), bob(5), carol(7.5), dave(10).
    assert client("ZRANK", "lb", "dave") == 3
    assert client("ZREVRANK", "lb", "dave") == 0
    assert client("ZREVRANK", "lb", "carol") == 1
    assert client("ZREVRANK", "lb", "bob") == 2
    assert client("ZREVRANK", "lb", "alice") == 3
    # ZREVRANK is read-only: the set is untouched.
    assert client("ZRANGE", "lb", "0", "-1") == ["alice", "bob", "carol", "dave"]


def test_zrevrank_withscore(client):
    _seed(client)
    assert client("ZREVRANK", "lb", "carol", "WITHSCORE") == [1, "7.5"]
    assert client("ZREVRANK", "lb", "alice", "WITHSCORE") == [3, "5.0"]


def test_zrevrank_missing_and_bad_args(client):
    _seed(client)
    assert client("ZREVRANK", "nope", "alice") is None
    assert client("ZREVRANK", "lb", "nobody") is None
    assert client("ZREVRANK", "lb")[0] == "ERROR"
    assert client("ZREVRANK", "lb", "alice", "BOGUS")[0] == "ERROR"
    assert client("ZREVRANK", "lb", "alice", "WITHSCORE", "extra")[0] == "ERROR"


def test_zrevrank_wrongtype(client):
    assert client("SET", "str", "v") == "OK"
    assert client("ZREVRANK", "str", "v")[0] == "ERROR"

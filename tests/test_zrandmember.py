"""Tests for tiny-redis ZRANDMEMBER. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_zrevrank.py.
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


def test_zrandmember_single_is_a_member(client):
    _seed(client)
    assert client("ZRANDMEMBER", "lb") in ("bob", "carol", "alice", "dave")


def test_zrandmember_missing_key(client):
    assert client("ZRANDMEMBER", "nope") is None
    assert client("ZRANDMEMBER", "nope", "3") == []


def test_zrandmember_positive_count_distinct_and_capped(client):
    _seed(client)
    picked = client("ZRANDMEMBER", "lb", "2")
    assert len(picked) == 2 and len(set(picked)) == 2
    assert set(picked) <= {"bob", "carol", "alice", "dave"}
    assert len(client("ZRANDMEMBER", "lb", "10")) == 4


def test_zrandmember_negative_count_allows_repeats(client):
    _seed(client)
    picked = client("ZRANDMEMBER", "lb", "-5")
    assert len(picked) == 5
    assert set(picked) <= {"bob", "carol", "alice", "dave"}


def test_zrandmember_withscores_pairs(client):
    _seed(client)
    pairs = client("ZRANDMEMBER", "lb", "2", "WITHSCORES")
    assert len(pairs) == 2
    scores = {"bob": "5.0", "carol": "7.5", "alice": "5.0", "dave": "10.0"}
    for member, score in pairs:
        assert scores[member] == score


def test_zrandmember_errors(client):
    _seed(client)
    assert client("SET", "s", "v") == "OK"
    assert client("ZRANDMEMBER", "s")[0] == "ERROR"  # WRONGTYPE
    assert client("ZRANDMEMBER", "lb", "abc")[0] == "ERROR"
    assert client("ZRANDMEMBER", "lb", "WITHSCORES")[0] == "ERROR"
    assert client("ZRANDMEMBER", "lb", "2", "BOGUS")[0] == "ERROR"
    assert client("ZRANDMEMBER")[0] == "ERROR"

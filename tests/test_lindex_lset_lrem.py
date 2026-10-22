"""Tests for tiny-redis LINDEX, LSET, LREM. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_ltrim.py.
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
            return [_read_reply() for _ in range(int(rest))]
        raise AssertionError(f"unknown reply type {kind!r}")

    yield cmd
    sock.close()
    server.shutdown()
    server.server_close()


def _push_five(client):
    assert client("RPUSH", "k", "a", "b", "c", "d", "e") == 5


# -- LINDEX ------------------------------------------------------------------


def test_lindex_basic(client):
    _push_five(client)
    assert client("LINDEX", "k", "0") == "a"
    assert client("LINDEX", "k", "2") == "c"
    assert client("LINDEX", "k", "4") == "e"


def test_lindex_negative(client):
    _push_five(client)
    assert client("LINDEX", "k", "-1") == "e"
    assert client("LINDEX", "k", "-5") == "a"


def test_lindex_out_of_range_is_nil(client):
    _push_five(client)
    assert client("LINDEX", "k", "5") is None
    assert client("LINDEX", "k", "-6") is None


def test_lindex_missing_key_is_nil(client):
    assert client("LINDEX", "nope", "0") is None


def test_lindex_wrongtype(client):
    client("SET", "s", "hello")
    kind, msg = client("LINDEX", "s", "0")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


# -- LSET --------------------------------------------------------------------


def test_lset_basic(client):
    _push_five(client)
    assert client("LSET", "k", "1", "B") == "OK"
    assert client("LRANGE", "k", "0", "-1") == ["a", "B", "c", "d", "e"]


def test_lset_negative_index(client):
    _push_five(client)
    assert client("LSET", "k", "-1", "E") == "OK"
    assert client("LINDEX", "k", "4") == "E"


def test_lset_out_of_range_errors(client):
    _push_five(client)
    kind, msg = client("LSET", "k", "5", "x")
    assert kind == "ERROR"
    assert "out of range" in msg


def test_lset_missing_key_errors(client):
    kind, msg = client("LSET", "nope", "0", "x")
    assert kind == "ERROR"
    assert "no such key" in msg


def test_lset_wrongtype(client):
    client("SET", "s", "hello")
    kind, msg = client("LSET", "s", "0", "x")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


# -- LREM --------------------------------------------------------------------


def test_lrem_count_positive_from_head(client):
    assert client("RPUSH", "r", "x", "a", "x", "b", "x") == 5
    assert client("LREM", "r", "2", "x") == 2
    assert client("LRANGE", "r", "0", "-1") == ["a", "b", "x"]


def test_lrem_count_negative_from_tail(client):
    assert client("RPUSH", "r", "x", "a", "x", "b", "x") == 5
    assert client("LREM", "r", "-2", "x") == 2
    assert client("LRANGE", "r", "0", "-1") == ["x", "a", "b"]


def test_lrem_count_zero_removes_all(client):
    assert client("RPUSH", "r", "x", "a", "x", "b", "x") == 5
    assert client("LREM", "r", "0", "x") == 3
    assert client("LRANGE", "r", "0", "-1") == ["a", "b"]


def test_lrem_element_not_found(client):
    _push_five(client)
    assert client("LREM", "k", "0", "z") == 0
    assert client("LRANGE", "k", "0", "-1") == ["a", "b", "c", "d", "e"]


def test_lrem_count_larger_than_occurrences(client):
    assert client("RPUSH", "r", "x", "a", "x") == 3
    assert client("LREM", "r", "10", "x") == 2
    assert client("LRANGE", "r", "0", "-1") == ["a"]


def test_lrem_missing_key(client):
    assert client("LREM", "nope", "0", "x") == 0


def test_lrem_wrongtype(client):
    client("SET", "s", "hello")
    kind, msg = client("LREM", "s", "0", "x")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg

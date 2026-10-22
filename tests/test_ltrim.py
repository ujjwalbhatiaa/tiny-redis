"""Tests for tiny-redis LTRIM. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_tiny_redis.py.
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


def test_ltrim_basic_middle(client):
    _push_five(client)
    assert client("LTRIM", "k", "1", "3") == "OK"
    assert client("LRANGE", "k", "0", "-1") == ["b", "c", "d"]


def test_ltrim_negative_stop(client):
    _push_five(client)
    assert client("LTRIM", "k", "0", "-2") == "OK"
    assert client("LRANGE", "k", "0", "-1") == ["a", "b", "c", "d"]


def test_ltrim_out_of_range_clamps(client):
    _push_five(client)
    assert client("LTRIM", "k", "-100", "100") == "OK"
    assert client("LRANGE", "k", "0", "-1") == ["a", "b", "c", "d", "e"]


def test_ltrim_empty_range_deletes_key(client):
    _push_five(client)
    assert client("LTRIM", "k", "3", "1") == "OK"  # start > stop
    assert client("EXISTS", "k") == 0
    assert client("LLEN", "k") == 0


def test_ltrim_start_past_end_deletes_key(client):
    _push_five(client)
    assert client("LTRIM", "k", "10", "20") == "OK"
    assert client("EXISTS", "k") == 0


def test_ltrim_missing_key_is_noop(client):
    assert client("LTRIM", "nope", "0", "-1") == "OK"
    assert client("EXISTS", "nope") == 0


def test_ltrim_wrongtype(client):
    client("SET", "s", "hello")
    kind, msg = client("LTRIM", "s", "0", "-1")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


def test_ltrim_preserves_ttl(client):
    _push_five(client)
    assert client("EXPIRE", "k", "100") == 1
    assert client("LTRIM", "k", "0", "1") == "OK"
    assert client("LRANGE", "k", "0", "-1") == ["a", "b"]
    assert 0 < client("TTL", "k") <= 100


def test_ltrim_arity(client):
    kind, msg = client("LTRIM", "k", "0")
    assert kind == "ERROR"
    assert "wrong number of arguments" in msg

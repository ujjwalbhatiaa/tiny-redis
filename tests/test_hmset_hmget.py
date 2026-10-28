"""Tests for tiny-redis HMSET / HMGET. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_hsetnx_hkeys_hvals.py.
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
        raise AssertionError("unknown reply kind: %r" % kind)

    yield cmd
    sock.close()
    server.shutdown()


# -- HMSET ---------------------------------------------------------------

def test_hmset_creates_hash_and_returns_ok(client):
    assert client("HMSET", "h", "a", "1", "b", "2") == "OK"
    assert client("HGETALL", "h") == ["a", "1", "b", "2"]


def test_hmset_overwrites_existing_fields(client):
    client("HSET", "h", "a", "old")
    assert client("HMSET", "h", "a", "new", "c", "3") == "OK"
    assert client("HGET", "h", "a") == "new"
    assert client("HGET", "h", "c") == "3"


def test_hmset_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, _msg = client("HMSET", "s", "a", "1")
    assert kind == "ERROR"


def test_hmset_odd_field_value_count_is_error(client):
    kind, _msg = client("HMSET", "h", "a")
    assert kind == "ERROR"


# -- HMGET ---------------------------------------------------------------

def test_hmget_preserves_request_order(client):
    client("HMSET", "h", "a", "1", "b", "2")
    assert client("HMGET", "h", "b", "a") == ["2", "1"]


def test_hmget_missing_field_is_none(client):
    client("HMSET", "h", "a", "1")
    assert client("HMGET", "h", "a", "nope") == ["1", None]


def test_hmget_missing_key_is_all_none(client):
    assert client("HMGET", "ghost", "a", "b") == [None, None]


def test_hmget_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, _msg = client("HMGET", "s", "a")
    assert kind == "ERROR"

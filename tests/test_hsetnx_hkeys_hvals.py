"""Tests for tiny-redis HSETNX / HKEYS / HVALS. Run with: pytest.

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
        raise AssertionError("unknown reply kind: %r" % kind)

    yield cmd
    sock.close()
    server.shutdown()


# -- HSETNX ---------------------------------------------------------------


def test_hsetnx_creates_key_and_returns_1(client):
    assert client("HSETNX", "h", "f", "v") == 1
    assert client("HGET", "h", "f") == "v"
    assert client("TYPE", "h") == "hash"


def test_hsetnx_new_field_on_existing_hash_returns_1(client):
    client("HSET", "h", "a", "1")
    assert client("HSETNX", "h", "b", "2") == 1
    assert client("HGET", "h", "b") == "2"


def test_hsetnx_existing_field_returns_0_and_keeps_value(client):
    client("HSET", "h", "f", "original")
    assert client("HSETNX", "h", "f", "changed") == 0
    assert client("HGET", "h", "f") == "original"


def test_hsetnx_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, msg = client("HSETNX", "s", "f", "v")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_hsetnx_arity(client):
    kind, _ = client("HSETNX", "h", "f")
    assert kind == "ERROR"
    kind, _ = client("HSETNX", "h")
    assert kind == "ERROR"


# -- HKEYS ----------------------------------------------------------------


def test_hkeys_returns_sorted_fields(client):
    client("HSET", "h", "zebra", "1", "apple", "2", "mango", "3")
    assert client("HKEYS", "h") == ["apple", "mango", "zebra"]


def test_hkeys_single_field(client):
    client("HSET", "h", "only", "v")
    assert client("HKEYS", "h") == ["only"]


def test_hkeys_missing_key_returns_empty(client):
    assert client("HKEYS", "nope") == []


def test_hkeys_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, msg = client("HKEYS", "s")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_hkeys_arity(client):
    kind, _ = client("HKEYS")
    assert kind == "ERROR"


# -- HVALS ----------------------------------------------------------------


def test_hvals_values_aligned_with_sorted_fields(client):
    client("HSET", "h", "zebra", "1", "apple", "2", "mango", "3")
    assert client("HVALS", "h") == ["2", "3", "1"]


def test_hvals_missing_key_returns_empty(client):
    assert client("HVALS", "nope") == []


def test_hvals_matches_hgetall_pairing(client):
    client("HSET", "h", "b", "20", "a", "10")
    flat = client("HGETALL", "h")
    assert flat == ["a", "10", "b", "20"]
    assert client("HKEYS", "h") == flat[0::2]
    assert client("HVALS", "h") == flat[1::2]


def test_hvals_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, msg = client("HVALS", "s")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_hvals_arity(client):
    kind, _ = client("HVALS")
    assert kind == "ERROR"


def test_hsetnx_then_hkeys_hvals_round_trip(client):
    assert client("HSETNX", "h", "plan", "pro") == 1
    assert client("HSETNX", "h", "plan", "free") == 0
    assert client("HSETNX", "h", "region", "ca") == 1
    assert client("HKEYS", "h") == ["plan", "region"]
    assert client("HVALS", "h") == ["pro", "ca"]

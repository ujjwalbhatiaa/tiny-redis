"""Tests for tiny-redis GETRANGE / SETRANGE. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_getdel_getex.py.
"""

import socket
import threading
import time

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
        raise AssertionError("unexpected reply kind: %r" % kind)

    yield cmd
    sock.close()
    server.shutdown()


def test_getrange_basic_and_negative_indices(client):
    assert client("SET", "k", "Hello, World!") == "OK"
    assert client("GETRANGE", "k", "0", "4") == "Hello"
    assert client("GETRANGE", "k", "-6", "-1") == "World!"
    assert client("GETRANGE", "k", "7", "100") == "World!"


def test_getrange_out_of_range_returns_empty(client):
    assert client("SET", "k", "abc") == "OK"
    assert client("GETRANGE", "k", "5", "9") == ""
    assert client("GETRANGE", "k", "2", "1") == ""
    assert client("GETRANGE", "missing", "0", "-1") == ""


def test_getrange_wrong_type_errors(client):
    assert client("RPUSH", "lst", "a") == 1
    kind, msg = client("GETRANGE", "lst", "0", "-1")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_getrange_rejects_non_integer(client):
    assert client("SET", "k", "abc") == "OK"
    kind, msg = client("GETRANGE", "k", "x", "2")
    assert kind == "ERROR"


def test_setrange_overwrites_middle_and_returns_length(client):
    assert client("SET", "k", "Hello World") == "OK"
    assert client("SETRANGE", "k", "6", "Redis") == 11
    assert client("GET", "k") == "Hello Redis"


def test_setrange_appends_and_pads_with_zero_bytes(client):
    assert client("SETRANGE", "new", "0", "hi") == 2
    assert client("GET", "new") == "hi"
    assert client("SETRANGE", "new", "5", "x") == 6
    assert client("GET", "new") == "hi\x00\x00\x00x"


def test_setrange_preserves_ttl(client):
    assert client("SET", "k", "abcdef", "EX", "100") == "OK"
    assert client("SETRANGE", "k", "0", "XY") == 6
    assert client("GET", "k") == "XYcdef"
    ttl = client("TTL", "k")
    assert 0 < ttl <= 100


def test_setrange_negative_offset_and_wrong_type(client):
    assert client("SET", "k", "abc") == "OK"
    kind, msg = client("SETRANGE", "k", "-1", "x")
    assert kind == "ERROR" and "out of range" in msg
    assert client("GET", "k") == "abc"
    assert client("RPUSH", "lst", "a") == 1
    kind, msg = client("SETRANGE", "lst", "0", "x")
    assert kind == "ERROR" and "WRONGTYPE" in msg

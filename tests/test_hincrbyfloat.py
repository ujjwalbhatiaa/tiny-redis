"""Tests for tiny-redis HINCRBYFLOAT. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_hmset_hmget.py.
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


def test_hincrbyfloat_creates_hash_and_field(client):
    assert client("HINCRBYFLOAT", "h", "score", "1.5") == "1.5"
    assert client("HGET", "h", "score") == "1.5"


def test_hincrbyfloat_accumulates_and_accepts_negative(client):
    client("HSET", "h", "score", "2.5")
    assert client("HINCRBYFLOAT", "h", "score", "1.25") == "3.75"
    assert client("HINCRBYFLOAT", "h", "score", "-0.75") == "3.0"


def test_hincrbyfloat_integer_delta_stays_float_string(client):
    client("HSET", "h", "n", "10")
    assert client("HINCRBYFLOAT", "h", "n", "2") == "12.0"


def test_hincrbyfloat_rejects_non_float_field_and_delta(client):
    client("HSET", "h", "s", "abc")
    kind, _msg = client("HINCRBYFLOAT", "h", "s", "1.5")
    assert kind == "ERROR"
    kind, _msg = client("HINCRBYFLOAT", "h", "n", "notanumber")
    assert kind == "ERROR"


def test_hincrbyfloat_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, _msg = client("HINCRBYFLOAT", "s", "f", "1.5")
    assert kind == "ERROR"


def test_hincrbyfloat_rejects_infinity(client):
    kind, _msg = client("HINCRBYFLOAT", "h", "f", "inf")
    assert kind == "ERROR"

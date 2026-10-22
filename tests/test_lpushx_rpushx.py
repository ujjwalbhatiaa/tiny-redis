"""Tests for tiny-redis LPUSHX / RPUSHX. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_lindex_lset_lrem.py.
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


# -- LPUSHX -------------------------------------------------------------------


def test_lpushx_missing_key_is_noop(client):
    assert client("LPUSHX", "nope", "x") == 0
    assert client("EXISTS", "nope") == 0  # key NOT created
    assert client("LLEN", "nope") == 0


def test_lpushx_existing_key_prepends(client):
    assert client("RPUSH", "k", "a", "b") == 2
    assert client("LPUSHX", "k", "z") == 3
    assert client("LRANGE", "k", "0", "-1") == ["z", "a", "b"]


def test_lpushx_multiple_elements_keep_push_order(client):
    assert client("RPUSH", "k", "a") == 1
    # each element is prepended in turn, like real Redis LPUSHX
    assert client("LPUSHX", "k", "x", "y") == 3
    assert client("LRANGE", "k", "0", "-1") == ["y", "x", "a"]


def test_lpushx_wrongtype(client):
    client("SET", "s", "hello")
    kind, msg = client("LPUSHX", "s", "x")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


def test_lpushx_expired_key_behaves_as_missing(client):
    client("RPUSH", "k", "a")
    client("EXPIRE", "k", "0")
    assert client("LPUSHX", "k", "x") == 0
    assert client("EXISTS", "k") == 0


def test_lpushx_preserves_ttl(client):
    client("RPUSH", "k", "a")
    client("EXPIRE", "k", "100")
    assert client("LPUSHX", "k", "b") == 2
    assert client("TTL", "k") in (99, 100)


def test_lpushx_arity_error(client):
    kind, msg = client("LPUSHX", "k")
    assert kind == "ERROR"
    assert "wrong number of arguments" in msg


# -- RPUSHX -------------------------------------------------------------------


def test_rpushx_missing_key_is_noop(client):
    assert client("RPUSHX", "nope", "x") == 0
    assert client("EXISTS", "nope") == 0  # key NOT created


def test_rpushx_existing_key_appends(client):
    assert client("LPUSH", "k", "b", "a") == 2
    assert client("RPUSHX", "k", "z") == 3
    assert client("LRANGE", "k", "0", "-1") == ["a", "b", "z"]


def test_rpushx_multiple_elements_keep_append_order(client):
    assert client("LPUSH", "k", "a") == 1
    assert client("RPUSHX", "k", "x", "y") == 3
    assert client("LRANGE", "k", "0", "-1") == ["a", "x", "y"]


def test_rpushx_wrongtype(client):
    client("HSET", "h", "f", "v")
    kind, msg = client("RPUSHX", "h", "x")
    assert kind == "ERROR"
    assert "WRONGTYPE" in msg


def test_rpushx_expired_key_behaves_as_missing(client):
    client("LPUSH", "k", "a")
    client("PEXPIRE", "k", "0")
    assert client("RPUSHX", "k", "x") == 0
    assert client("EXISTS", "k") == 0


def test_rpushx_does_not_clobber_existing_list(client):
    # 13-element list survives a round of conditional pushes intact
    vals = [f"v{i}" for i in range(13)]
    assert client("RPUSH", "k", *vals) == 13
    assert client("LPUSHX", "k", "head") == 14
    assert client("RPUSHX", "k", "tail") == 15
    assert client("LRANGE", "k", "0", "-1") == ["head"] + vals + ["tail"]

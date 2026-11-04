"""Tests for tiny-redis ZMSCORE. Run with: pytest.

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
    assert client("ZADD", "lb", "5", "alice", "7.5", "carol",
                  "10", "dave") == 3


def test_zmscore_present_and_missing_members(client):
    _seed(client)
    assert client("ZMSCORE", "lb", "alice", "carol", "dave") == [
        "5.0", "7.5", "10.0"]
    assert client("ZMSCORE", "lb", "alice", "nobody", "dave") == [
        "5.0", None, "10.0"]
    # Order follows the request, not score order.
    assert client("ZMSCORE", "lb", "dave", "alice") == ["10.0", "5.0"]


def test_zmscore_missing_key_is_all_nil(client):
    assert client("ZMSCORE", "nope", "alice", "bob") == [None, None]
    # A missing key stays missing: ZMSCORE is read-only.
    assert client("ZMSCORE", "nope", "alice") == [None]
    assert client("EXISTS", "nope") == 0


def test_zmscore_bad_args_and_wrongtype(client):
    _seed(client)
    assert client("ZMSCORE", "lb")[0] == "ERROR"
    assert client("ZMSCORE")[0] == "ERROR"
    assert client("SET", "str", "v") == "OK"
    assert client("ZMSCORE", "str", "v")[0] == "ERROR"

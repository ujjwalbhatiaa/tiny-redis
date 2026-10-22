"""Tests for tiny-redis ZINCRBY / ZCOUNT / ZREVRANGE. Run with: pytest.

Spins up a real server on an ephemeral port and speaks RESP to it,
same harness as test_lpushx_rpushx.py.
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


# -- ZINCRBY ---------------------------------------------------------------


def test_zincrby_creates_key_and_member(client):
    assert client("ZINCRBY", "z", "2.5", "a") == "2.5"
    assert client("ZSCORE", "z", "a") == "2.5"


def test_zincrby_increments_existing(client):
    client("ZADD", "z", "1", "a")
    assert client("ZINCRBY", "z", "3", "a") == "4.0"
    assert client("ZINCRBY", "z", "-1.5", "a") == "2.5"


def test_zincrby_float_increment_round_trip(client):
    # 0.1 + 0.2 style: shortest round-trip string, like ZSCORE/INCRBYFLOAT
    assert client("ZINCRBY", "z", "0.1", "a") == "0.1"
    out = client("ZINCRBY", "z", "0.2", "a")
    assert out == repr(0.1 + 0.2)


def test_zincrby_rejects_non_finite_result(client):
    client("ZADD", "z", "1e308", "a")
    kind, msg = client("ZINCRBY", "z", "1e308", "a")
    assert kind == "ERROR" and "NaN or Infinity" in msg


def test_zincrby_rejects_bad_increment(client):
    kind, msg = client("ZINCRBY", "z", "abc", "a")
    assert kind == "ERROR" and "not a valid float" in msg


def test_zincrby_wrongtype_on_string_key(client):
    client("SET", "s", "v")
    kind, msg = client("ZINCRBY", "s", "1", "a")
    assert kind == "ERROR" and "WRONGTYPE" in msg


def test_zincrby_arity(client):
    kind, _ = client("ZINCRBY", "z", "1")
    assert kind == "ERROR"


def test_zincrby_preserves_ttl(client):
    client("ZADD", "z", "1", "a")
    client("EXPIRE", "z", "100")
    client("ZINCRBY", "z", "1", "a")
    assert client("TTL", "z") > 0


# -- ZCOUNT -----------------------------------------------------------------


def test_zcount_basic_range(client):
    client("ZADD", "z", "1", "a", "2", "b", "3", "c")
    assert client("ZCOUNT", "z", "1", "2") == 2
    assert client("ZCOUNT", "z", "0", "10") == 3


def test_zcount_infinities(client):
    client("ZADD", "z", "1", "a", "5", "b")
    assert client("ZCOUNT", "z", "-inf", "+inf") == 2
    assert client("ZCOUNT", "z", "-inf", "1") == 1


def test_zcount_exclusive_bound(client):
    client("ZADD", "z", "1", "a", "2", "b", "3", "c")
    assert client("ZCOUNT", "z", "(1", "3") == 2
    assert client("ZCOUNT", "z", "1", "(3") == 2
    assert client("ZCOUNT", "z", "(1", "(3") == 1


def test_zcount_missing_key_is_zero(client):
    assert client("ZCOUNT", "nope", "-inf", "+inf") == 0


def test_zcount_rejects_bad_bound(client):
    kind, msg = client("ZCOUNT", "z", "x", "2")
    assert kind == "ERROR" and "not a float" in msg


def test_zcount_wrongtype(client):
    client("SET", "s", "v")
    kind, msg = client("ZCOUNT", "s", "-inf", "+inf")
    assert kind == "ERROR" and "WRONGTYPE" in msg


# -- ZREVRANGE ---------------------------------------------------------------


def test_zrevrange_basic_order(client):
    client("ZADD", "z", "1", "a", "2", "b", "3", "c")
    assert client("ZREVRANGE", "z", "0", "-1") == ["c", "b", "a"]
    assert client("ZREVRANGE", "z", "0", "1") == ["c", "b"]


def test_zrevrange_negative_indexes(client):
    client("ZADD", "z", "1", "a", "2", "b", "3", "c")
    assert client("ZREVRANGE", "z", "-2", "-1") == ["b", "a"]
    assert client("ZREVRANGE", "z", "5", "10") == []


def test_zrevrange_withscores(client):
    client("ZADD", "z", "1", "a", "2.5", "b")
    assert client("ZREVRANGE", "z", "0", "-1", "WITHSCORES") == ["b", "2.5", "a", "1.0"]


def test_zrevrange_bad_withscores_arg(client):
    client("ZADD", "z", "1", "a")
    kind, msg = client("ZREVRANGE", "z", "0", "-1", "BOGUS")
    assert kind == "ERROR" and "syntax error" in msg


def test_zrevrange_missing_key_empty(client):
    assert client("ZREVRANGE", "nope", "0", "-1") == []


def test_zrevrange_wrongtype(client):
    client("SET", "s", "v")
    kind, msg = client("ZREVRANGE", "s", "0", "-1")
    assert kind == "ERROR" and "WRONGTYPE" in msg

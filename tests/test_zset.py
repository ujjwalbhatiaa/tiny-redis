"""Tests for tiny-redis sorted sets. Run with: pytest.

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


def test_zadd_and_zrange_ordering(client):
    # returns number of NEW members added
    assert client("ZADD", "lb", "10", "alice", "5", "bob", "7.5", "carol") == 3
    # re-adding an existing member updates the score, adds 0
    assert client("ZADD", "lb", "1", "alice") == 0
    # ZRANGE returns members lowest-score first; ties broken by member name
    assert client("ZRANGE", "lb", "0", "-1") == ["alice", "bob", "carol"]
    assert client("ZRANGE", "lb", "0", "1") == ["alice", "bob"]
    assert client("ZRANGE", "lb", "-2", "-1") == ["bob", "carol"]


def test_zadd_score_tie_breaks_by_member(client):
    client("ZADD", "t", "1", "zed", "1", "amy", "1", "mid")
    assert client("ZRANGE", "t", "0", "-1") == ["amy", "mid", "zed"]


def test_zscore(client):
    client("ZADD", "lb", "2.5", "alice")
    assert client("ZSCORE", "lb", "alice") == "2.5"
    assert client("ZSCORE", "lb", "nobody") is None
    assert client("ZSCORE", "missing", "alice") is None


def test_zcard_and_type(client):
    assert client("ZCARD", "missing") == 0
    client("ZADD", "lb", "1", "a", "2", "b")
    assert client("ZCARD", "lb") == 2
    assert client("TYPE", "lb") == "zset"


def test_zrem(client):
    client("ZADD", "lb", "1", "a", "2", "b", "3", "c")
    assert client("ZREM", "lb", "a", "zzz") == 1
    assert client("ZRANGE", "lb", "0", "-1") == ["b", "c"]
    assert client("ZREM", "missing", "a") == 0
    assert client("ZCARD", "lb") == 2


def test_zrank(client):
    client("ZADD", "lb", "30", "c", "10", "a", "20", "b")
    assert client("ZRANK", "lb", "a") == 0
    assert client("ZRANK", "lb", "b") == 1
    assert client("ZRANK", "lb", "c") == 2
    assert client("ZRANK", "lb", "nobody") is None
    assert client("ZRANK", "missing", "a") is None


def test_zset_wrongtype(client):
    client("SET", "str", "x")
    kind, msg = client("ZADD", "str", "1", "m")
    assert kind == "ERROR" and "WRONGTYPE" in msg
    kind, _ = client("ZSCORE", "str", "m")
    assert kind == "ERROR"
    kind, _ = client("ZRANGE", "str", "0", "-1")
    assert kind == "ERROR"
    kind, _ = client("ZCARD", "str")
    assert kind == "ERROR"
    kind, _ = client("ZREM", "str", "m")
    assert kind == "ERROR"
    kind, _ = client("ZRANK", "str", "m")
    assert kind == "ERROR"
    # the string key is untouched
    assert client("GET", "str") == "x"


def test_zadd_arity_and_bad_scores(client):
    kind, _ = client("ZADD", "lb", "1")
    assert kind == "ERROR"
    kind, _ = client("ZADD", "lb", "1", "a", "2")
    assert kind == "ERROR"  # odd arg count
    kind, msg = client("ZADD", "lb", "notascore", "a")
    assert kind == "ERROR" and "not a valid float" in msg
    kind, msg = client("ZADD", "lb", "inf", "a")
    assert kind == "ERROR" and "not a valid float" in msg
    kind, _ = client("ZSCORE", "lb")
    assert kind == "ERROR"
    kind, _ = client("ZRANGE", "lb", "0")
    assert kind == "ERROR"
    kind, msg = client("ZRANGE", "lb", "x", "y")
    assert kind == "ERROR" and "not an integer" in msg


def test_zadd_keeps_ttl(client):
    client("ZADD", "lb", "1", "a")
    client("EXPIRE", "lb", "100", )
    assert client("ZADD", "lb", "2", "b") == 1
    assert 0 < client("TTL", "lb") <= 100
    assert client("ZRANGE", "lb", "0", "-1") == ["a", "b"]


def test_zset_survives_save_and_load(tmp_path):
    dbfile = str(tmp_path / "dump.rdb")
    db = Database()
    db.execute([b"ZADD", b"lb", b"10", b"alice", b"5", b"bob"])
    db.save(dbfile)
    db2 = Database()
    db2.load(dbfile)
    assert db2.execute([b"ZRANGE", b"lb", b"0", b"-1"]) == ["bob", "alice"]
    assert db2.execute([b"ZSCORE", b"lb", b"alice"]) == "10.0"

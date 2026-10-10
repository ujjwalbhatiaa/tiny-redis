# tiny-redis

A tiny Redis clone in pure Python. It speaks the RESP protocol, so the real
`redis-cli` can talk to it. Standard library only.

## Run it

```bash
python tiny_redis.py --port 6379
```

Then in another terminal (if you have `redis-cli`):

```
$ redis-cli -p 6379
127.0.0.1:6379> SET name ujjwal
OK
127.0.0.1:6379> GET name
"ujjwal"
127.0.0.1:6379> RPUSH todos "ship project" "write tests"
(integer) 2
```

## Supported commands

| Group   | Commands |
|---------|----------|
| Connection | `PING`, `ECHO`, `QUIT` |
| Strings | `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `APPEND`, `STRLEN`, `MSET`, `MGET`, `SETNX`, `GETSET`, `MSETNX` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` |
| Keys | `KEYS` (glob), `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `FLUSHDB`, `DBSIZE` |
| Server | `INFO`, `SAVE`, `COMMAND` |

## How it works

- **RESP**: a small incremental parser reads arrays of bulk strings off the
  socket; replies are serialized as simple strings, errors, integers, bulk
  strings, and arrays.
- **Storage**: one dict maps keys to `(type, value, expire_at)` tuples.
  Expiry is checked lazily on access (`TTL`/`GET`/… return the key as
  missing once past its deadline).
- **Concurrency**: `ThreadingMixIn` TCP server with a re-entrant lock around
  the keyspace, so concurrent clients are safe.
- **Persistence**: `SAVE` pickles live (unexpired) keys to `dump.rdb`;
  the server reloads it on startup. `SET … EX`/`PX` deadlines survive the
  round-trip as absolute timestamps.

## Benchmark

```bash
python benchmark.py
```

On this machine: ~36k SET/sec and ~39k GET/sec over loopback
(your numbers will vary — it's Python, not C).

## Tests

```bash
pytest
```

23 tests: every command group over a real socket, expiry timing, glob
`KEYS`, `WRONGTYPE` errors, unknown commands, multi-key set/get (`MSET`/`MGET`
— nulls for missing keys, overwrite of non-string keys, arity errors),
conditional set (`SETNX`, `GETSET`, `MSETNX` — all-or-nothing semantics,
no-overwrite guarantees), and
save/load round-trips including expiry filtering.

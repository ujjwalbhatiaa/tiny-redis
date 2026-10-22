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

| Group | Commands |
|---------|----------|
| Connection | `PING`, `ECHO`, `QUIT` |
| Strings | `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `MSET`, `MGET`, `SETNX`, `GETSET`, `MSETNX` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LTRIM`, `LINDEX`, `LSET`, `LREM`, `LPUSHX`, `RPUSHX` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SINTER`, `SUNION`, `SDIFF` |
| Hashes | `HSET`, `HSETNX`, `HGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HINCRBY` |
| Sorted sets | `ZADD`, `ZSCORE`, `ZRANGE`, `ZREVRANGE` (`WITHSCORES`), `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZREM`, `ZRANK` |
| Keys | `KEYS` (glob), `SCAN` (cursor, `MATCH`/`COUNT`), `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `FLUSHDB`, `DBSIZE` |
| Server | `INFO`, `SAVE`, `COMMAND` |

## How it works

- **RESP**: a small incremental parser reads arrays of bulk strings off the
 socket; replies are serialized as simple strings, errors, integers, bulk
strings, and arrays.

- **Storage**: one dict maps keys to `(type, value, expire_at)` tuples.
 Expiry is checked lazily on access (`TTL`/`GET`/… return the key as
 missing once past its deadline). Sorted sets store a `member -> score`
 dict; ordering is by `(score, member)`.

- **Concurrency**: `ThreadingMixIn` TCP server with a re-entrant lock around
 the keyspace, so concurrent clients are safe.

- **Iteration**: `KEYS` blocks on a full glob scan; `SCAN` pages through the
 keyspace instead — an integer cursor plus optional `MATCH` glob and `COUNT`
 page-size hint, returning cursor `"0"` when the iteration completes. The
 snapshot is rebuilt per call (sorted, so concurrent deletions don't shift
 offsets), and like real Redis, keys added or removed mid-iteration may be
 visited twice or skipped.

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

54 tests: every command group over a real socket, expiry timing, glob
`KEYS`, cursor `SCAN` (full-iteration collection, `MATCH`/`COUNT` filtering,
expired-key skipping, empty-DB and cursor/option error cases), `WRONGTYPE`
errors, unknown commands, multi-key set/get (`MSET`/`MGET`
— nulls for missing keys, overwrite of non-string keys, arity errors),
conditional set (`SETNX`, `GETSET`, `MSETNX` — all-or-nothing semantics,
no-overwrite guarantees), numeric ops (`INCRBYFLOAT` float formatting and
error cases, `HINCRBY` field creation, negative deltas, `WRONGTYPE` on
non-hash keys), hash field introspection (`HSETNX` no-overwrite guarantee,
`HKEYS`/`HVALS` sorted-by-field pairing with `HGETALL`, missing-key empty
arrays), set algebra (`SINTER`/`SUNION`/`SDIFF` — empty-set behavior
for missing keys, no mutation of source sets, `WRONGTYPE` on non-set keys),
sorted sets (`ZADD` batch add/update, `ZRANGE`/`ZREVRANGE` score ordering
with negative-index ranges, `ZSCORE`/`ZCARD`/`ZCOUNT` (incl. `-inf`/`+inf`
and exclusive `(x` bounds)/`ZINCRBY`/`ZREM`/`ZRANK`, `WRONGTYPE` on
non-zset keys, TTL preservation, save/load round-trip),
list trimming (`LTRIM` — negative-index/out-of-range clamping, empty-range
key deletion, missing-key no-op, TTL preservation, `WRONGTYPE`),
list element access and mutation (`LINDEX` — negative-index/nil-on-miss,
`LSET` — negative-index set with `no such key`/`index out of range` errors,
`LREM` — head/tail/all removal by count sign, `WRONGTYPE`),
conditional push (`LPUSHX`/`RPUSHX` — no-op 0 on missing/expired keys,
`WRONGTYPE` on non-list keys, multi-element order),
and save/load round-trips including expiry filtering.

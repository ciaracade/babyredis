# babyredis

🧱 Redis-like commands, SQLite underneath, no server to run.

> [!NOTE]
> In progress. Open source contributions welcome.

babyredis is a [redis-py](https://github.com/redis/redis-py)-shaped client
backed by a single SQLite file (WAL mode). Think
[redka](https://github.com/nalgeon/redka), but pure Python, in-process, and
pip-installable with **zero dependencies** — `sqlite3` ships with Python.

```python
from babyredis import Redis

r = Redis("cache.db")          # or Redis(":memory:") for ephemeral
r.set("session:42", "ciara", ex=3600)
r.get("session:42")            # b'ciara'
r.incr("page:views")           # 1
r.ttl("session:42")            # 3600
```

## Why?

- **No server.** No daemon to install, configure, monitor, or pay for. Your
  cache is a file next to your app — perfect for single-server deployments,
  CLIs, scripts, and side projects.
- **Survives restarts.** Unlike a dict (or fakeredis), data persists and can
  be shared across processes on the same machine.
- **Familiar API.** Method signatures mirror redis-py (`set` with
  `ex`/`px`/`nx`/`xx`/`keepttl`/`get`, `ttl` returning -2/-1, bytes responses
  with a `decode_responses` flag), so swapping a small app off Redis — or
  onto it later — is mostly an import change.

Honest trade-offs: local SQLite reads are fast (no TCP round-trip), but
Redis will beat this on write throughput and tail latency, and there's no
cross-machine networking. If you need pub/sub, clustering, or six-figure
ops/sec, run real Redis. This is for everyone who doesn't.

## Supported commands (v0.3)

**Strings:** `set`, `get`, `getset`, `getdel`, `setex`, `psetex`, `setnx`,
`append`, `strlen`, `mset`, `mget`, `incr`/`incrby`, `decr`/`decrby`,
`incrbyfloat`.

**Hashes:** `hset` (with `mapping=`), `hget`, `hgetall`, `hdel`, `hexists`,
`hkeys`, `hvals`, `hlen`, `hmget`, `hsetnx`, `hincrby`, `hincrbyfloat`,
`hstrlen`, `hscan`/`hscan_iter`.

**Sets:** `sadd`, `srem`, `smembers`, `sismember`, `smismember`, `scard`,
`spop`, `srandmember`, `smove`, `sinter`, `sunion`, `sdiff`,
`sinterstore`/`sunionstore`/`sdiffstore`, `sscan`/`sscan_iter`.

**Sorted sets:** `zadd` (`nx`/`xx`/`gt`/`lt`/`ch`/`incr`), `zincrby`,
`zscore`, `zmscore`, `zrem`, `zcard`, `zcount`, `zrange`/`zrevrange`,
`zrangebyscore`/`zrevrangebyscore` (with `(`-exclusive and `±inf` bounds),
`zrank`/`zrevrank`, `zpopmin`/`zpopmax`,
`zremrangebyrank`/`zremrangebyscore`,
`zunionstore`/`zinterstore` (with weights and SUM/MIN/MAX aggregation;
plain sets participate at score 1.0), `zscan`/`zscan_iter`.

**Lists:** `lpush`, `rpush`, `lpop`/`rpop` (with `count`), `llen`,
`lrange`, `lindex`, `lset`, `lrem`, `ltrim`, `linsert`, `lmove`,
`rpoplpush`.

**Keys & server:** `delete`, `exists`, `expire`, `pexpire`, `expireat`,
`persist`, `ttl`, `pttl`, `keys` (Redis glob patterns),
`scan`/`scan_iter`, `rename`, `renamenx`, `randomkey`, `type`, `dbsize`,
`flushdb`, `ping`.

**Pipelines:** `pipeline()` queues commands and runs them in a single
SQLite transaction on `execute()` — unlike real Redis MULTI/EXEC, the
batch is fully ACID: an error rolls back the whole batch.

```python
with r.pipeline() as pipe:
    pipe.set("a", 1).incr("hits").rpush("log", "x")
    results = pipe.execute()   # [True, 1, 1]
```

Semantics follow Redis: operations against a key of the wrong type raise
`ResponseError` (WRONGTYPE), removing a collection's last element removes
the key, and TTLs apply per key across all types. Expired keys are
invisible immediately and physically purged lazily, with a periodic sweep
on writes.

## Concurrency

File-backed databases use one SQLite connection per thread: reads take no
Python lock (WAL allows concurrent readers) and writes serialize through
SQLite's own locking. Cross-process access to the same file is safe —
writes are atomic via immediate transactions. `:memory:` databases use a
single lock-guarded connection (SQLite memory databases can't be shared
across connections).

## Compatibility validation

`tests/test_redka_compat.py` ports the edge-case suite from
[redka](https://github.com/nalgeon/redka)'s Go tests (score tie-breaking,
list range/trim boundary permutations, store-variant overwrite semantics,
atomic `mset` rollback, and more). Where redka deviates from Redis —
silent wrong-type reads, keeping emptied keys, refusing cross-type
`rename`, no negative `zrange` indices — babyredis follows **Redis**, and
each divergence is noted in the test.

On top of that, `tests/test_oracle.py` runs property-based tests with
[fakeredis](https://github.com/cunla/fakeredis-py) as the oracle: a
Hypothesis state machine fires random command sequences at both clients
and asserts identical results (or identical failures) after every step.

## Testing

babyredis doubles as a Redis stand-in for tests. Opt into the bundled
fixtures:

```python
# conftest.py
pytest_plugins = ["babyredis.testing"]

def test_something(babyredis_client):
    babyredis_client.set("k", "v")
```

## Install

```sh
pip install babyredis
```

## Roadmap

- Published benchmarks vs Redis and fakeredis
- `redis.asyncio`-shaped async client
- Type hints + `py.typed`

## Development

```sh
pip install -e .
pip install pytest hypothesis fakeredis
pytest tests/
```

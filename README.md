<div align="center">
  <img src="docs/logo.png" alt="babyredis logo" width="320">

  <h1>babyredis</h1>

  <p><strong>Redis-like commands. SQLite underneath. No server to run.</strong></p>

  <p>
    <a href="https://github.com/ciaracade/babyredis/actions/workflows/test.yml">
      <img src="https://github.com/ciaracade/babyredis/actions/workflows/test.yml/badge.svg" alt="Tests">
    </a>
    <img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+">
    <img src="https://img.shields.io/badge/dependencies-zero-brightgreen" alt="Zero dependencies">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License">
  </p>
</div>

---

babyredis is a [redis-py](https://github.com/redis/redis-py)-shaped client
backed by a single SQLite file. Think
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

No daemon to install, configure, monitor, or pay for. Your cache is a file
next to your app.

## Features

- 🧱 **~80 Redis commands** — strings, hashes, sets, sorted sets, lists,
  TTLs, scan cursors, pipelines
- 🔌 **redis-py compatible API** — same method signatures, same return
  conventions (`bytes` responses, `decode_responses=True` for `str`,
  `ttl` returning `-2`/`-1`, `WRONGTYPE` errors)
- 💾 **Survives restarts** — data lives in a WAL-mode SQLite file, not a
  process
- 🤝 **Cross-process safe** — multiple processes share one file; writes
  are atomic via immediate transactions
- ⚡ **Fast where it matters** — no TCP round trip means single-op reads
  beat Redis-over-loopback ~6–10x ([benchmarks](docs/performance.md))
- 🧪 **Triple-validated** — unit tests, an edge-case suite ported from
  redka's Go tests, and property-based differential testing against
  fakeredis
- ⏳ **Async support** — `babyredis.aio` mirrors `redis.asyncio`
- 🐍 **Zero dependencies** — stdlib only, Python 3.9+

## Installation

```sh
pip install babyredis
```

## Usage

### The basics

```python
from babyredis import Redis

r = Redis("app.db", decode_responses=True)

# strings & counters
r.set("user:1:name", "alice", ex=3600)
r.incr("hits")
r.incrbyfloat("balance", 9.99)

# hashes
r.hset("user:1", mapping={"name": "alice", "plan": "pro"})
r.hgetall("user:1")            # {'name': 'alice', 'plan': 'pro'}

# sorted sets
r.zadd("leaderboard", {"alice": 4200, "bob": 1380})
r.zrevrange("leaderboard", 0, 9, withscores=True)

# lists
r.rpush("queue", "job-1", "job-2")
r.lpop("queue")                # 'job-1'

# iteration
for key in r.scan_iter(match="user:*"):
    ...
```

### Pipelines

`pipeline()` queues commands and runs them in a single SQLite transaction —
unlike real Redis MULTI/EXEC, the batch is fully ACID: an error rolls back
the whole batch.

```python
with r.pipeline() as pipe:
    pipe.set("a", 1).incr("hits").rpush("log", "x")
    results = pipe.execute()   # [True, 1, 1]
```

### Async

`babyredis.aio` mirrors `redis.asyncio`: same constructor, every command
awaitable, `*_iter` helpers are async generators. Commands run in a worker
thread so the event loop never blocks on SQLite I/O.

```python
from babyredis.aio import Redis

async with Redis("cache.db") as r:
    await r.set("k", "v", ex=60)
    await r.get("k")
    async for key in r.scan_iter(match="user:*"):
        ...
```

### As a test double

babyredis doubles as a Redis stand-in for tests — like fakeredis, but with
real persistence if your tests cross a process boundary:

```python
# conftest.py
pytest_plugins = ["babyredis.testing"]

def test_something(babyredis_client):
    babyredis_client.set("k", "v")
```

## Supported commands

<details>
<summary><strong>Click to expand the full command list (v0.5)</strong></summary>

**Strings:** `set` (`ex`/`px`/`exat`/`pxat`/`nx`/`xx`/`keepttl`/`get`),
`get`, `getset`, `getdel`, `setex`, `psetex`, `setnx`, `append`, `strlen`,
`mset`, `mget`, `incr`/`incrby`, `decr`/`decrby`, `incrbyfloat`.

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
`zremrangebyrank`/`zremrangebyscore`, `zunionstore`/`zinterstore` (with
weights and SUM/MIN/MAX aggregation; plain sets participate at score 1.0),
`zscan`/`zscan_iter`.

**Lists:** `lpush`, `rpush`, `lpop`/`rpop` (with `count`), `llen`,
`lrange`, `lindex`, `lset`, `lrem`, `ltrim`, `linsert`, `lmove`,
`rpoplpush`.

**Keys & server:** `delete`, `exists`, `expire`, `pexpire`, `expireat`,
`persist`, `ttl`, `pttl`, `keys` (Redis glob patterns),
`scan`/`scan_iter`, `rename`, `renamenx`, `randomkey`, `type`, `dbsize`,
`flushdb`, `ping`.

</details>

Semantics follow Redis: operations against a key of the wrong type raise
`ResponseError` (WRONGTYPE), removing a collection's last element removes
the key, and TTLs apply per key across all types. Expired keys are
invisible immediately and physically purged lazily, with a periodic sweep
on writes.

## Performance

Median µs per operation, single thread — full table, methodology, and
honest caveats in [docs/performance.md](docs/performance.md):

| | babyredis (file) | fakeredis | Redis (loopback) |
|---|---|---|---|
| GET | **11.6** | 67.1 | 125.3 |
| SET | **19.4** | 90.9 | 129.9 |
| ZADD | **34.6** | 99.3 | 131.0 |
| PIPELINE SET ×100 | 14.8 | 56.9 | **13.9** |

The honest framing: no TCP round trip means babyredis wins single-op
latency; pipelining erases that edge; a plain dict beats everything by
~40x; and real Redis wins concurrent write throughput (SQLite allows one
writer at a time). If you need pub/sub, clustering, or six-figure ops/sec,
run real Redis. This is for everyone who doesn't.

Reproduce with `python benchmarks/bench.py`.

## How it works

A master `babyredis_keys` table owns each key's name, type, TTL, and
element count; one child table per data type holds the payload and
cascades on delete. This keeps expiry and `DEL` in one place for every
type, makes type mismatches fail with `WRONGTYPE` like real Redis, and
keeps `llen`/`hlen`/`scard`/`zcard` O(1).

**Concurrency:** file-backed databases use one SQLite connection per
thread — reads take no Python lock (WAL allows concurrent readers) and
writes serialize through SQLite's own locking via immediate transactions.
Cross-process access to the same file is safe. `:memory:` databases use a
single lock-guarded connection, since SQLite memory databases can't be
shared across connections.

## Correctness

Three layers of validation, run on every push across 15 OS/Python
combinations:

1. **Unit tests** for every command, including TTL behavior, persistence
   across reopen, thread safety, and multi-process atomicity.
2. **[redka](https://github.com/nalgeon/redka)'s edge-case suite**, ported
   from its Go tests ([`tests/test_redka_compat.py`](tests/test_redka_compat.py)):
   score tie-breaking, list boundary permutations, store-variant overwrite
   semantics, atomic `mset` rollback. Where redka deviates from Redis,
   babyredis follows **Redis**, with each divergence noted.
3. **Property-based differential testing**
   ([`tests/test_oracle.py`](tests/test_oracle.py)): a Hypothesis state
   machine fires random command sequences at both
   [fakeredis](https://github.com/cunla/fakeredis-py) and babyredis,
   asserting identical results — or identical failures — after every step.

## Why not just …?

| If you need | Use |
|---|---|
| A same-process cache, no persistence | a `dict` or `functools.lru_cache` — 40x faster |
| To fake Redis in unit tests | [fakeredis](https://github.com/cunla/fakeredis-py) — broader command coverage |
| A mature SQLite cache (its own API) | [diskcache](https://github.com/grantjenks/python-diskcache) |
| Pub/sub, Lua, clustering, networked access | real [Redis](https://redis.io)/[Valkey](https://valkey.io) |
| Redis-shaped persistence with no server, in Python | **babyredis** |

The longer story: [Redis re-implemented with SQLite](https://news.ycombinator.com/item?id=40030746)
struck a nerve (502 points), Rails 8 shipped
[Redis-free defaults on SQL](https://rubyonrails.org/2024/11/7/rails-8-no-paas-required),
and Python had no equivalent — fakeredis is memory-only, redislite embeds
a real Redis binary and is unmaintained, redka is Go-only. babyredis fills
that gap.

## Development

```sh
git clone https://github.com/ciaracade/babyredis
cd babyredis
pip install -e .
pip install pytest hypothesis fakeredis
pytest tests/
```

Contributions welcome — the [roadmap](#roadmap) below is a good place to
start, and the three-layer test setup means you'll know quickly whether a
change holds up.

## Roadmap

- [ ] Type hints + `py.typed`
- [ ] `expire` flags (`nx`/`xx`/`gt`/`lt`), `getex`, `setrange`/`getrange`
- [ ] PyPI release

## Acknowledgements

- [redka](https://github.com/nalgeon/redka) by Anton Zhiyanov — proof the
  idea works, and the source of our ported edge-case suite
- [fakeredis](https://github.com/cunla/fakeredis-py) — the correctness
  oracle for our property tests
- [diskcache](https://github.com/grantjenks/python-diskcache) — prior art
  for honest SQLite-vs-Redis benchmarking

## License

[MIT](LICENSE) © [Ciara Cade](https://ciaracade.com)

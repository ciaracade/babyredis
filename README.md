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

## Supported commands (v0.2)

**Strings:** `set`, `get`, `getset`, `getdel`, `setex`, `psetex`, `setnx`,
`append`, `strlen`, `mset`, `mget`, `incr`/`incrby`, `decr`/`decrby`.

**Hashes:** `hset` (with `mapping=`), `hget`, `hgetall`, `hdel`, `hexists`,
`hkeys`, `hvals`, `hlen`, `hmget`, `hsetnx`, `hincrby`, `hstrlen`.

**Keys & server:** `delete`, `exists`, `expire`, `pexpire`, `expireat`,
`persist`, `ttl`, `pttl`, `keys` (Redis glob patterns), `type`, `dbsize`,
`flushdb`, `ping`.

Semantics follow Redis: operations against a key of the wrong type raise
`ResponseError` (WRONGTYPE), deleting a hash's last field removes the key,
and TTLs apply per key across all types. Expired keys are invisible
immediately and physically purged lazily, with a periodic sweep on writes.

## Install

```sh
pip install babyredis
```

## Roadmap

- Sets, sorted sets, lists
- `pipeline()` (one SQLite transaction per pipeline — actually ACID)
- `scan`/`hscan` iteration
- Lock-free concurrent reads via thread-local connections
- An optional pytest fixture for using babyredis as a test double

## Development

```sh
pip install -e . pytest
pytest tests/
```

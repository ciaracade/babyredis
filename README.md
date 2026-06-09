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

## Supported commands (v0.1)

Strings and keys: `set`, `get`, `getset`, `getdel`, `setex`, `psetex`,
`setnx`, `append`, `strlen`, `mset`, `mget`, `incr`/`incrby`,
`decr`/`decrby`, `delete`, `exists`, `expire`, `pexpire`, `expireat`,
`persist`, `ttl`, `pttl`, `keys` (Redis glob patterns), `type`, `dbsize`,
`flushdb`, `ping`.

Expired keys are invisible immediately and physically purged lazily
(Redis-style), with a periodic sweep on writes.

## Install

```sh
pip install babyredis
```

## Roadmap

- Hashes (`hset`/`hget`/...), lists, sets
- `scan` iteration
- An optional pytest fixture for using babyredis as a test double

## Development

```sh
pip install -e . pytest
pytest tests/
```

# Performance

Honest numbers, honestly framed. babyredis is in-process SQLite, so it
wins on single-operation latency against anything that crosses a socket —
and loses to a plain dict by ~40x, to real Redis on concurrent write
throughput, and it can't talk across machines at all. Pick accordingly.

## Results

Median microseconds per operation (p99 in parentheses), 5,000 ops per
command after warmup, single thread. Run on a 4-core Intel Xeon @ 2.80GHz
(x86_64, Linux), Python 3.11, Redis 7 over TCP loopback with persistence
disabled, fakeredis 2.36 in-process.

| op | babyredis (file/WAL) | babyredis (:memory:) | fakeredis | Redis (TCP loopback) | dict |
|---|---|---|---|---|---|
| SET | 19.4 (69) | 15.6 (59) | 90.9 (169) | 129.9 (204) | 0.4 (1) |
| GET | 11.6 (49) | 7.9 (32) | 67.1 (126) | 125.3 (212) | 0.3 (1) |
| INCR | 25.2 (76) | 16.3 (52) | 75.3 (135) | 124.2 (206) | 0.3 (0) |
| HSET | 15.9 (49) | 12.4 (44) | 82.1 (144) | 126.2 (222) | 0.4 (1) |
| HGET | 12.0 (44) | 8.2 (29) | 70.9 (151) | 127.5 (222) | 0.3 (0) |
| ZADD | 34.6 (104) | 19.9 (54) | 99.3 (200) | 131.0 (235) | 0.4 (1) |
| ZRANGE 10 | 19.7 (50) | 15.8 (45) | 109.5 (195) | 147.9 (254) | 59.9 (114) |
| LPUSH | 31.8 (99) | 20.5 (57) | 74.0 (130) | 124.3 (223) | 1.1 (3) |
| RPOP | 30.3 (88) | 19.4 (54) | 71.8 (123) | 122.2 (222) | 0.2 (0) |
| PIPELINE SET x100 | 14.8 (16) | 14.8 (17) | 56.9 (62) | 13.9 (18) | — |

Reproduce with:

```sh
pip install fakeredis redis
python benchmarks/bench.py --n 5000 --redis-url redis://localhost:6379
```

## Reading the table

- **babyredis beats Redis-over-loopback ~6-10x on single-op latency.**
  This is architecture, not engine quality: every Redis command pays a
  TCP round trip plus protocol serialization (~110 µs of the ~125 µs
  here); babyredis is a function call into SQLite. The same effect is
  documented in [diskcache's benchmarks](https://grantjenks.com/docs/diskcache/cache-benchmarks.html).
- **Pipelining erases Redis's overhead.** At 100 commands per batch the
  round trip amortizes away and Redis matches babyredis (~14 µs/op).
  If your workload is large batches against a hot server, Redis is fine.
- **A dict is ~40x faster than any of this.** If you need a same-process
  cache with no persistence and no cross-process sharing, use a dict (or
  `functools.lru_cache`). babyredis buys you durability, TTLs, atomic
  cross-process operations, and Redis-shaped data types — not raw speed.
- **`:memory:` is only ~1.3-1.6x faster than the file.** WAL mode plus
  the OS page cache keeps warm-file reads near memory speed; the file
  mode's durability is cheap. This mirrors published SQLite benchmarks.
- **Write throughput under concurrency is the known weak spot.** SQLite
  allows one writer at a time; a write-heavy multi-process workload will
  serialize where Redis would not. [Redka's benchmarks](https://github.com/nalgeon/redka/blob/main/docs/performance.md)
  show the same shape for the same reason.

## Caveats

- Single-threaded, localhost, small values (5-6 byte payloads). Tail
  latencies on the file backend include occasional fsync stalls that a
  short table can't show; expect worst-case writes in the milliseconds.
- fakeredis is a pure-Python emulator built for testing, not speed;
  it's in the table because it's the closest drop-in alternative, not
  because beating it is impressive.
- Micro-benchmarks are not your workload. Measure your own.

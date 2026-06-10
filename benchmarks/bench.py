"""Benchmark babyredis against a plain dict, fakeredis, and real Redis.

Usage:
    python benchmarks/bench.py [--n 5000] [--redis-url redis://localhost:6379]

Real Redis is skipped unless --redis-url is given and reachable. Results
print as a Markdown table of median (p50) microseconds per operation, with
p99 in parentheses.
"""

import argparse
import statistics
import tempfile
import time


def bench(fn, n, batch=1):
    """Run fn n times, return list of per-op latencies in microseconds."""
    samples = []
    for i in range(n):
        t0 = time.perf_counter()
        fn(i)
        samples.append((time.perf_counter() - t0) / batch * 1e6)
    return samples


class DictContestant:
    name = "dict"

    def __init__(self):
        self.d = {}
        self.h = {}
        self.z = {}
        self.l = []

    def ops(self):
        d, h, z, l = self.d, self.h, self.z, self.l
        return {
            "SET": lambda i: d.__setitem__(f"k{i % 1000}", b"value"),
            "GET": lambda i: d.get(f"k{i % 1000}"),
            "INCR": lambda i: d.__setitem__("n", d.get("n", 0) + 1),
            "HSET": lambda i: h.__setitem__(f"f{i % 100}", b"v"),
            "HGET": lambda i: h.get(f"f{i % 100}"),
            "ZADD": lambda i: z.__setitem__(f"m{i % 1000}", float(i)),
            "ZRANGE 10": lambda i: sorted(z.items(), key=lambda kv: kv[1])[:10],
            "LPUSH": lambda i: l.insert(0, b"v"),
            "RPOP": lambda i: l.pop() if l else None,
        }

    def pipeline_setter(self):
        return None

    def close(self):
        pass


class ClientContestant:
    def __init__(self, name, client):
        self.name = name
        self.r = client

    def ops(self):
        r = self.r
        return {
            "SET": lambda i: r.set(f"k{i % 1000}", b"value"),
            "GET": lambda i: r.get(f"k{i % 1000}"),
            "INCR": lambda i: r.incr("n"),
            "HSET": lambda i: r.hset("h", f"f{i % 100}", b"v"),
            "HGET": lambda i: r.hget("h", f"f{i % 100}"),
            "ZADD": lambda i: r.zadd("z", {f"m{i % 1000}": float(i)}),
            "ZRANGE 10": lambda i: r.zrange("z", 0, 9),
            "LPUSH": lambda i: r.lpush("l", b"v"),
            "RPOP": lambda i: r.rpop("l"),
        }

    def pipeline_setter(self):
        r = self.r

        def run(i):
            pipe = r.pipeline()
            for j in range(100):
                pipe.set(f"p{j}", b"value")
            pipe.execute()
        return run

    def close(self):
        try:
            self.r.flushdb()
            self.r.close()
        except Exception:
            pass


def contestants(args, tmpdir):
    from babyredis import BabyRedis
    yield ClientContestant("babyredis (file/WAL)",
                           BabyRedis(f"{tmpdir}/bench.db"))
    yield ClientContestant("babyredis (:memory:)", BabyRedis(":memory:"))
    try:
        import fakeredis
        yield ClientContestant("fakeredis", fakeredis.FakeRedis())
    except ImportError:
        pass
    if args.redis_url:
        import redis
        client = redis.Redis.from_url(args.redis_url)
        try:
            client.ping()
        except redis.exceptions.RedisError:
            print(f"(skipping real Redis: {args.redis_url} not reachable)")
        else:
            yield ClientContestant("Redis (TCP loopback)", client)
    yield DictContestant()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=5000)
    parser.add_argument("--redis-url", default=None)
    args = parser.parse_args()

    results = {}  # op -> {contestant: (p50, p99)}
    names = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for c in contestants(args, tmpdir):
            names.append(c.name)
            for op, fn in c.ops().items():
                bench(fn, min(500, args.n))  # warmup
                samples = bench(fn, args.n)
                results.setdefault(op, {})[c.name] = (
                    statistics.median(samples),
                    statistics.quantiles(samples, n=100)[98],
                )
            pipe_fn = c.pipeline_setter()
            if pipe_fn is not None:
                samples = bench(pipe_fn, max(50, args.n // 100), batch=100)
                results.setdefault("PIPELINE SET x100", {})[c.name] = (
                    statistics.median(samples),
                    statistics.quantiles(samples, n=100)[98],
                )
            c.close()

    header = "| op | " + " | ".join(names) + " |"
    sep = "|---" * (len(names) + 1) + "|"
    print()
    print(header)
    print(sep)
    for op, per in results.items():
        cells = []
        for name in names:
            if name in per:
                p50, p99 = per[name]
                cells.append(f"{p50:.1f} ({p99:.0f})")
            else:
                cells.append("—")
        print(f"| {op} | " + " | ".join(cells) + " |")
    print()
    print(f"p50 µs/op (p99 in parentheses), n={args.n} per op")


if __name__ == "__main__":
    main()

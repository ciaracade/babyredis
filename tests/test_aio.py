"""Tests for the async client (plain asyncio.run, no pytest plugin)."""

import asyncio

import pytest

from babyredis import ResponseError
from babyredis.aio import BabyRedis, Redis


def test_redis_alias():
    assert Redis is BabyRedis


def test_basic_roundtrip(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db")) as r:
            assert await r.set("k", "v", ex=60) is True
            assert await r.get("k") == b"v"
            assert await r.incr("n") == 1
            assert 0 < await r.ttl("k") <= 60
            assert await r.hset("h", mapping={"a": 1, "b": 2}) == 2
            assert await r.hgetall("h") == {b"a": b"1", b"b": b"2"}
            assert await r.zadd("z", {"m": 1.5}) == 1
            assert await r.zscore("z", "m") == 1.5
            assert await r.rpush("l", "x", "y") == 2
            assert await r.lrange("l", 0, -1) == [b"x", b"y"]
    asyncio.run(main())


def test_errors_propagate(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db")) as r:
            await r.set("s", "text")
            with pytest.raises(ResponseError):
                await r.incr("s")
            with pytest.raises(ResponseError):
                await r.hget("s", "f")
    asyncio.run(main())


def test_pipeline(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db")) as r:
            async with r.pipeline() as pipe:
                pipe.set("a", "1").incr("n").rpush("l", "x")
                assert len(pipe) == 3
                results = await pipe.execute()
            assert results == [True, 1, 1]
            assert await r.get("a") == b"1"
    asyncio.run(main())


def test_scan_iters(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db")) as r:
            for i in range(25):
                await r.set(f"k{i}", i)
            collected = {key async for key in r.scan_iter(count=7)}
            assert collected == {f"k{i}".encode() for i in range(25)}
            await r.hset("h", mapping={f"f{i}": i for i in range(15)})
            fields = {f async for f, _ in r.hscan_iter("h", count=4)}
            assert len(fields) == 15
            await r.sadd("s", *(f"m{i}" for i in range(15)))
            members = {m async for m in r.sscan_iter("s", count=4)}
            assert len(members) == 15
            await r.zadd("z", {f"m{i}": i for i in range(15)})
            scored = {m: s async for m, s in r.zscan_iter("z", count=4)}
            assert scored[b"m3"] == 3.0
    asyncio.run(main())


def test_concurrent_increments(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db")) as r:
            async def worker():
                for _ in range(20):
                    await r.incr("counter")
            await asyncio.gather(*(worker() for _ in range(10)))
            assert await r.get("counter") == b"200"
    asyncio.run(main())


def test_memory_mode():
    async def main():
        async with BabyRedis(":memory:") as r:
            await r.set("k", "v")
            assert await r.get("k") == b"v"
    asyncio.run(main())


def test_decode_responses(tmp_path):
    async def main():
        async with BabyRedis(str(tmp_path / "a.db"),
                             decode_responses=True) as r:
            await r.set("k", "héllo")
            assert await r.get("k") == "héllo"
            assert r.decode_responses is True
    asyncio.run(main())

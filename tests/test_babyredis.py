import threading
import time
from datetime import timedelta

import pytest

from babyredis import BabyRedis, DataError, Redis, ResponseError


@pytest.fixture
def r(tmp_path):
    client = BabyRedis(str(tmp_path / "test.db"))
    yield client
    client.close()


@pytest.fixture
def rd(tmp_path):
    client = BabyRedis(str(tmp_path / "test.db"), decode_responses=True)
    yield client
    client.close()


class TestSetGet:
    def test_set_get_roundtrip(self, r):
        assert r.set("k", "v") is True
        assert r.get("k") == b"v"

    def test_get_missing_returns_none(self, r):
        assert r.get("nope") is None

    def test_bytes_and_str_keys_are_same_key(self, r):
        r.set(b"k", "v1")
        assert r.get("k") == b"v1"

    def test_int_and_float_values(self, r):
        r.set("i", 42)
        r.set("f", 3.5)
        assert r.get("i") == b"42"
        assert r.get("f") == b"3.5"

    def test_binary_value(self, r):
        blob = bytes(range(256))
        r.set("b", blob)
        assert r.get("b") == blob

    def test_invalid_value_type_raises(self, r):
        with pytest.raises(DataError):
            r.set("k", ["nope"])
        with pytest.raises(DataError):
            r.set("k", True)

    def test_decode_responses(self, rd):
        rd.set("k", "héllo")
        assert rd.get("k") == "héllo"

    def test_nx(self, r):
        assert r.set("k", "v1", nx=True) is True
        assert r.set("k", "v2", nx=True) is None
        assert r.get("k") == b"v1"

    def test_xx(self, r):
        assert r.set("k", "v1", xx=True) is None
        assert r.get("k") is None
        r.set("k", "v1")
        assert r.set("k", "v2", xx=True) is True
        assert r.get("k") == b"v2"

    def test_set_with_get_flag(self, r):
        assert r.set("k", "v1", get=True) is None
        assert r.set("k", "v2", get=True) == b"v1"

    def test_set_nx_with_get_returns_old_without_writing(self, r):
        r.set("k", "v1")
        assert r.set("k", "v2", nx=True, get=True) == b"v1"
        assert r.get("k") == b"v1"

    def test_keepttl(self, r):
        r.set("k", "v1", ex=100)
        r.set("k", "v2", keepttl=True)
        assert r.ttl("k") > 0
        r.set("k", "v3")
        assert r.ttl("k") == -1


class TestExpiry:
    def test_px_expires(self, r):
        r.set("k", "v", px=50)
        assert r.get("k") == b"v"
        time.sleep(0.08)
        assert r.get("k") is None
        assert r.exists("k") == 0

    def test_ex_timedelta(self, r):
        r.set("k", "v", ex=timedelta(seconds=100))
        assert 0 < r.ttl("k") <= 100

    def test_ttl_states(self, r):
        assert r.ttl("missing") == -2
        r.set("k", "v")
        assert r.ttl("k") == -1
        r.expire("k", 100)
        assert 0 < r.ttl("k") <= 100
        assert 0 < r.pttl("k") <= 100_000

    def test_expire_missing_key(self, r):
        assert r.expire("missing", 10) is False

    def test_persist(self, r):
        r.set("k", "v", ex=100)
        assert r.persist("k") is True
        assert r.ttl("k") == -1
        assert r.persist("k") is False

    def test_expireat(self, r):
        r.set("k", "v")
        assert r.expireat("k", time.time() + 100) is True
        assert 0 < r.ttl("k") <= 100

    def test_setex_psetex_setnx(self, r):
        r.setex("a", 100, "v")
        assert 0 < r.ttl("a") <= 100
        r.psetex("b", 100_000, "v")
        assert 0 < r.pttl("b") <= 100_000
        assert r.setnx("c", "v1") is True
        assert r.setnx("c", "v2") is False

    def test_expired_keys_invisible_everywhere(self, r):
        r.set("k", "v", px=30)
        time.sleep(0.05)
        assert r.keys() == []
        assert r.dbsize() == 0
        assert r.delete("k") == 0


class TestCounters:
    def test_incr_from_missing(self, r):
        assert r.incr("n") == 1
        assert r.incr("n") == 2
        assert r.get("n") == b"2"

    def test_incrby_decr(self, r):
        assert r.incrby("n", 10) == 10
        assert r.decrby("n", 3) == 7
        assert r.decr("n") == 6

    def test_incr_non_integer_raises(self, r):
        r.set("k", "hello")
        with pytest.raises(ResponseError):
            r.incr("k")

    def test_incr_preserves_ttl(self, r):
        r.set("n", 1, ex=100)
        r.incr("n")
        assert 0 < r.ttl("n") <= 100


class TestKeys:
    def test_delete_and_exists(self, r):
        r.mset({"a": 1, "b": 2, "c": 3})
        assert r.exists("a", "b", "missing") == 2
        assert r.delete("a", "b", "missing") == 2
        assert r.exists("a") == 0

    def test_keys_patterns(self, r):
        r.mset({"user:1": "a", "user:2": "b", "order:1": "c"})
        assert sorted(r.keys("user:*")) == [b"user:1", b"user:2"]
        assert r.keys("user:?") == r.keys("user:*")
        assert sorted(r.keys()) == [b"order:1", b"user:1", b"user:2"]
        assert r.keys("user:[1]") == [b"user:1"]
        assert r.keys("user:[^1]") == [b"user:2"]

    def test_type(self, r):
        r.set("k", "v")
        assert r.type("k") == b"string"
        assert r.type("missing") == b"none"


class TestStringOps:
    def test_append_strlen(self, r):
        assert r.append("k", "Hello") == 5
        assert r.append("k", " World") == 11
        assert r.get("k") == b"Hello World"
        assert r.strlen("k") == 11
        assert r.strlen("missing") == 0

    def test_mset_mget(self, r):
        r.mset({"a": "1", "b": "2"})
        assert r.mget("a", "b", "missing") == [b"1", b"2", None]
        assert r.mget(["a", "b"]) == [b"1", b"2"]

    def test_getset_clears_ttl(self, r):
        r.set("k", "v1", ex=100)
        assert r.getset("k", "v2") == b"v1"
        assert r.get("k") == b"v2"
        assert r.ttl("k") == -1

    def test_getdel(self, r):
        r.set("k", "v")
        assert r.getdel("k") == b"v"
        assert r.get("k") is None
        assert r.getdel("missing") is None


class TestHashes:
    def test_hset_hget(self, r):
        assert r.hset("h", "f", "v") == 1
        assert r.hget("h", "f") == b"v"
        assert r.hget("h", "missing") is None
        assert r.hget("missing", "f") is None

    def test_hset_update_returns_zero(self, r):
        r.hset("h", "f", "v1")
        assert r.hset("h", "f", "v2") == 0
        assert r.hget("h", "f") == b"v2"

    def test_hset_mapping(self, r):
        assert r.hset("h", mapping={"a": 1, "b": 2}) == 2
        assert r.hset("h", "c", 3, mapping={"a": 9}) == 1
        assert r.hget("h", "a") == b"9"

    def test_hset_no_args_raises(self, r):
        with pytest.raises(DataError):
            r.hset("h")

    def test_hgetall(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hgetall("h") == {b"a": b"1", b"b": b"2"}
        assert r.hgetall("missing") == {}

    def test_hgetall_decode_responses(self, rd):
        rd.hset("h", mapping={"a": "1"})
        assert rd.hgetall("h") == {"a": "1"}

    def test_hdel(self, r):
        r.hset("h", mapping={"a": 1, "b": 2, "c": 3})
        assert r.hdel("h", "a", "b", "missing") == 2
        assert r.hlen("h") == 1
        assert r.hdel("missing", "f") == 0

    def test_hdel_last_field_removes_key(self, r):
        r.hset("h", "f", "v")
        r.hdel("h", "f")
        assert r.exists("h") == 0
        assert r.type("h") == b"none"

    def test_hexists_hkeys_hvals_hlen(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hexists("h", "a") is True
        assert r.hexists("h", "z") is False
        assert sorted(r.hkeys("h")) == [b"a", b"b"]
        assert sorted(r.hvals("h")) == [b"1", b"2"]
        assert r.hlen("h") == 2
        assert r.hlen("missing") == 0

    def test_hmget(self, r):
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hmget("h", "a", "z", "b") == [b"1", None, b"2"]
        assert r.hmget("h", ["a", "b"]) == [b"1", b"2"]
        assert r.hmget("missing", "a") == [None]

    def test_hsetnx(self, r):
        assert r.hsetnx("h", "f", "v1") is True
        assert r.hsetnx("h", "f", "v2") is False
        assert r.hget("h", "f") == b"v1"

    def test_hincrby(self, r):
        assert r.hincrby("h", "n") == 1
        assert r.hincrby("h", "n", 10) == 11
        assert r.hincrby("h", "n", -1) == 10
        r.hset("h", "s", "text")
        with pytest.raises(ResponseError):
            r.hincrby("h", "s")

    def test_hstrlen(self, r):
        r.hset("h", "f", "hello")
        assert r.hstrlen("h", "f") == 5
        assert r.hstrlen("h", "missing") == 0

    def test_hash_type_and_keys(self, r):
        r.hset("h", "f", "v")
        assert r.type("h") == b"hash"
        assert r.keys() == [b"h"]
        assert r.dbsize() == 1

    def test_hash_ttl(self, r):
        r.hset("h", "f", "v")
        assert r.ttl("h") == -1
        assert r.expire("h", 100) is True
        assert 0 < r.ttl("h") <= 100
        r.hset("gone", "f", "v")
        r.pexpire("gone", 30)
        time.sleep(0.05)
        assert r.hgetall("gone") == {}
        assert r.exists("gone") == 0

    def test_hash_persists_across_reopen(self, tmp_path):
        path = str(tmp_path / "h.db")
        with BabyRedis(path) as r1:
            r1.hset("h", mapping={"a": "1", "b": "2"})
        with BabyRedis(path) as r2:
            assert r2.hgetall("h") == {b"a": b"1", b"b": b"2"}


class TestSets:
    def test_sadd_smembers(self, r):
        assert r.sadd("s", "a", "b", "c") == 3
        assert r.sadd("s", "a", "d") == 1
        assert r.smembers("s") == {b"a", b"b", b"c", b"d"}

    def test_srem(self, r):
        r.sadd("s", "a", "b", "c")
        assert r.srem("s", "a", "b", "zz") == 2
        assert r.smembers("s") == {b"c"}
        assert r.srem("missing", "x") == 0

    def test_srem_last_member_removes_key(self, r):
        r.sadd("s", "a")
        r.srem("s", "a")
        assert r.exists("s") == 0

    def test_sismember_scard(self, r):
        r.sadd("s", "a", "b")
        assert r.sismember("s", "a") is True
        assert r.sismember("s", "z") is False
        assert r.scard("s") == 2
        assert r.scard("missing") == 0
        assert r.smismember("s", ["a", "z"]) == [True, False]

    def test_spop(self, r):
        r.sadd("s", "a", "b", "c")
        popped = r.spop("s")
        assert popped in (b"a", b"b", b"c")
        assert r.scard("s") == 2
        rest = r.spop("s", 5)
        assert len(rest) == 2
        assert r.exists("s") == 0
        assert r.spop("missing") is None
        assert r.spop("missing", 2) == []

    def test_srandmember(self, r):
        r.sadd("s", "a", "b")
        assert r.srandmember("s") in (b"a", b"b")
        assert len(r.srandmember("s", 5)) == 2
        assert len(r.srandmember("s", -5)) == 5
        assert r.scard("s") == 2  # srandmember doesn't remove
        assert r.srandmember("missing") is None

    def test_smove(self, r):
        r.sadd("src", "a", "b")
        r.sadd("dst", "x")
        assert r.smove("src", "dst", "a") is True
        assert r.smembers("src") == {b"b"}
        assert r.smembers("dst") == {b"a", b"x"}
        assert r.smove("src", "dst", "zz") is False

    def test_set_operations(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "b", "c", "d")
        assert r.sinter("s1", "s2") == {b"b", b"c"}
        assert r.sunion("s1", "s2") == {b"a", b"b", b"c", b"d"}
        assert r.sdiff("s1", "s2") == {b"a"}
        assert r.sinter(["s1", "s2"]) == {b"b", b"c"}
        assert r.sinter("s1", "missing") == set()

    def test_set_type_and_wrongtype(self, r):
        r.sadd("s", "a")
        assert r.type("s") == b"set"
        with pytest.raises(ResponseError):
            r.get("s")
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.sadd("str", "a")

    def test_sscan(self, r):
        members = {f"m{i}" for i in range(25)}
        r.sadd("s", *members)
        collected = set(r.sscan_iter("s", count=7))
        assert collected == {m.encode() for m in members}
        assert set(r.sscan_iter("s", match="m1*", count=7)) == {
            m.encode() for m in members if m.startswith("m1")}


class TestSortedSets:
    def test_zadd_zscore_zcard(self, r):
        assert r.zadd("z", {"a": 1, "b": 2}) == 2
        assert r.zadd("z", {"a": 5, "c": 3}) == 1
        assert r.zscore("z", "a") == 5.0
        assert r.zscore("z", "zz") is None
        assert r.zcard("z") == 3
        assert r.zmscore("z", ["a", "zz"]) == [5.0, None]

    def test_zadd_nx_xx_ch(self, r):
        r.zadd("z", {"a": 1})
        assert r.zadd("z", {"a": 9, "b": 2}, nx=True) == 1
        assert r.zscore("z", "a") == 1.0
        assert r.zadd("z", {"a": 9, "c": 3}, xx=True) == 0
        assert r.zscore("z", "a") == 9.0
        assert r.zscore("z", "c") is None
        assert r.zadd("z", {"a": 10, "d": 4}, ch=True) == 2

    def test_zadd_gt_lt(self, r):
        r.zadd("z", {"a": 5})
        r.zadd("z", {"a": 3}, gt=True)
        assert r.zscore("z", "a") == 5.0
        r.zadd("z", {"a": 7}, gt=True)
        assert r.zscore("z", "a") == 7.0
        r.zadd("z", {"a": 2}, lt=True)
        assert r.zscore("z", "a") == 2.0

    def test_zincrby(self, r):
        assert r.zincrby("z", 2.5, "a") == 2.5
        assert r.zincrby("z", 1.5, "a") == 4.0

    def test_zrem_removes_key_when_empty(self, r):
        r.zadd("z", {"a": 1, "b": 2})
        assert r.zrem("z", "a", "zz") == 1
        assert r.zrem("z", "b") == 1
        assert r.exists("z") == 0

    def test_zrange(self, r):
        r.zadd("z", {"a": 1, "b": 2, "c": 3})
        assert r.zrange("z", 0, -1) == [b"a", b"b", b"c"]
        assert r.zrange("z", 0, 1) == [b"a", b"b"]
        assert r.zrange("z", -2, -1) == [b"b", b"c"]
        assert r.zrevrange("z", 0, 0) == [b"c"]
        assert r.zrange("z", 0, -1, withscores=True) == [
            (b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]
        assert r.zrange("missing", 0, -1) == []

    def test_zrangebyscore(self, r):
        r.zadd("z", {"a": 1, "b": 2, "c": 3, "d": 4})
        assert r.zrangebyscore("z", 2, 3) == [b"b", b"c"]
        assert r.zrangebyscore("z", "(2", "+inf") == [b"c", b"d"]
        assert r.zrangebyscore("z", "-inf", "+inf", start=1, num=2) == [
            b"b", b"c"]
        assert r.zrevrangebyscore("z", 3, 1) == [b"c", b"b", b"a"]
        assert r.zcount("z", 2, "+inf") == 3

    def test_zrank(self, r):
        r.zadd("z", {"a": 1, "b": 2, "c": 3})
        assert r.zrank("z", "a") == 0
        assert r.zrank("z", "c") == 2
        assert r.zrevrank("z", "c") == 0
        assert r.zrank("z", "zz") is None
        assert r.zrank("missing", "a") is None

    def test_zpopmin_zpopmax(self, r):
        r.zadd("z", {"a": 1, "b": 2, "c": 3})
        assert r.zpopmin("z") == [(b"a", 1.0)]
        assert r.zpopmax("z", 2) == [(b"c", 3.0), (b"b", 2.0)]
        assert r.exists("z") == 0
        assert r.zpopmin("missing") == []

    def test_zset_type_and_wrongtype(self, r):
        r.zadd("z", {"a": 1})
        assert r.type("z") == b"zset"
        with pytest.raises(ResponseError):
            r.sadd("z", "x")

    def test_zscan(self, r):
        r.zadd("z", {f"m{i}": i for i in range(25)})
        collected = dict(r.zscan_iter("z", count=7))
        assert len(collected) == 25
        assert collected[b"m13"] == 13.0


class TestLists:
    def test_push_pop_order(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lrange("l", 0, -1) == [b"a", b"b", b"c"]
        r.lpush("l", "x", "y")  # y ends up leftmost
        assert r.lrange("l", 0, -1) == [b"y", b"x", b"a", b"b", b"c"]
        assert r.lpop("l") == b"y"
        assert r.rpop("l") == b"c"
        assert r.lrange("l", 0, -1) == [b"x", b"a", b"b"]

    def test_pop_count(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lpop("l", 2) == [b"a", b"b"]
        assert r.rpop("l", 5) == [b"c"]
        assert r.exists("l") == 0
        assert r.lpop("missing") is None
        assert r.lpop("missing", 2) is None

    def test_llen_lindex(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.llen("l") == 3
        assert r.llen("missing") == 0
        assert r.lindex("l", 0) == b"a"
        assert r.lindex("l", -1) == b"c"
        assert r.lindex("l", 9) is None

    def test_lrange_negative_indices(self, r):
        r.rpush("l", *"abcde")
        assert r.lrange("l", 1, 3) == [b"b", b"c", b"d"]
        assert r.lrange("l", -2, -1) == [b"d", b"e"]
        assert r.lrange("l", 3, 1) == []
        assert r.lrange("missing", 0, -1) == []

    def test_lset(self, r):
        r.rpush("l", "a", "b", "c")
        assert r.lset("l", 1, "B") is True
        assert r.lset("l", -1, "C") is True
        assert r.lrange("l", 0, -1) == [b"a", b"B", b"C"]
        with pytest.raises(ResponseError):
            r.lset("l", 9, "x")
        with pytest.raises(ResponseError):
            r.lset("missing", 0, "x")

    def test_lrem(self, r):
        r.rpush("l", "a", "b", "a", "c", "a")
        assert r.lrem("l", 2, "a") == 2
        assert r.lrange("l", 0, -1) == [b"b", b"c", b"a"]
        r.rpush("l", "a")
        assert r.lrem("l", -1, "a") == 1
        assert r.lrange("l", 0, -1) == [b"b", b"c", b"a"]
        assert r.lrem("l", 0, "a") == 1
        assert r.lrem("missing", 0, "x") == 0

    def test_ltrim(self, r):
        r.rpush("l", *"abcde")
        assert r.ltrim("l", 1, 3) is True
        assert r.lrange("l", 0, -1) == [b"b", b"c", b"d"]
        r.ltrim("l", 5, 9)  # keeps nothing
        assert r.exists("l") == 0

    def test_linsert(self, r):
        r.rpush("l", "a", "c")
        assert r.linsert("l", "before", "c", "b") == 3
        assert r.linsert("l", "after", "c", "d") == 4
        assert r.lrange("l", 0, -1) == [b"a", b"b", b"c", b"d"]
        assert r.linsert("l", "before", "zz", "x") == -1
        assert r.linsert("missing", "before", "a", "x") == 0

    def test_linsert_many_times_same_spot(self, r):
        # repeatedly bisecting the same gap exhausts float precision and
        # forces a renumber; order must survive
        r.rpush("l", "start", "end")
        for i in range(80):
            assert r.linsert("l", "after", "start", str(i)) == i + 3
        items = r.lrange("l", 0, -1)
        assert items[0] == b"start"
        assert items[-1] == b"end"
        assert items[1] == b"79"
        assert len(items) == 82

    def test_list_type_and_wrongtype(self, r):
        r.rpush("l", "a")
        assert r.type("l") == b"list"
        with pytest.raises(ResponseError):
            r.get("l")
        r.set("s", "v")
        with pytest.raises(ResponseError):
            r.rpush("s", "a")

    def test_list_persists_across_reopen(self, tmp_path):
        path = str(tmp_path / "l.db")
        with BabyRedis(path) as r1:
            r1.rpush("l", "a", "b", "c")
        with BabyRedis(path) as r2:
            assert r2.lrange("l", 0, -1) == [b"a", b"b", b"c"]


class TestPipeline:
    def test_pipeline_executes_in_order(self, r):
        with r.pipeline() as pipe:
            pipe.set("a", "1").incr("n").rpush("l", "x", "y")
            assert len(pipe) == 3
            results = pipe.execute()
        assert results == [True, 1, 2]
        assert r.get("a") == b"1"

    def test_pipeline_is_atomic_on_error(self, r):
        r.set("s", "not a number")
        pipe = r.pipeline()
        pipe.set("a", "1")
        pipe.incr("s")  # will raise ResponseError
        pipe.set("b", "2")
        with pytest.raises(ResponseError):
            pipe.execute()
        # the whole batch rolled back, including the first set
        assert r.get("a") is None
        assert r.get("b") is None

    def test_pipeline_raise_on_error_false(self, r):
        r.set("s", "not a number")
        pipe = r.pipeline()
        pipe.set("a", "1")
        pipe.incr("s")
        results = pipe.execute(raise_on_error=False)
        assert results[0] is True
        assert isinstance(results[1], ResponseError)
        assert r.get("a") == b"1"

    def test_pipeline_resets_after_execute(self, r):
        pipe = r.pipeline()
        pipe.set("a", "1")
        pipe.execute()
        assert len(pipe) == 0
        assert pipe.execute() == []


class TestScan:
    def test_scan_iterates_everything(self, r):
        for i in range(25):
            r.set(f"k{i}", i)
        assert set(r.scan_iter(count=7)) == {f"k{i}".encode()
                                             for i in range(25)}

    def test_scan_match(self, r):
        r.mset({"user:1": "a", "user:2": "b", "order:1": "c"})
        assert set(r.scan_iter(match="user:*")) == {b"user:1", b"user:2"}

    def test_scan_cursor_protocol(self, r):
        for i in range(5):
            r.set(f"k{i}", i)
        cursor, page = r.scan(0, count=2)
        assert cursor != 0 and len(page) == 2
        cursor2, page2 = r.scan(cursor, count=10)
        assert cursor2 == 0
        assert len(page) + len(page2) == 5

    def test_scan_skips_expired(self, r):
        r.set("dead", "v", px=20)
        r.set("alive", "v")
        time.sleep(0.05)
        assert list(r.scan_iter()) == [b"alive"]

    def test_hscan(self, r):
        r.hset("h", mapping={f"f{i}": i for i in range(25)})
        collected = dict(r.hscan_iter("h", count=7))
        assert len(collected) == 25
        assert collected[b"f3"] == b"3"
        matched = dict(r.hscan_iter("h", match="f1?", count=7))
        assert set(matched) == {f"f1{i}".encode() for i in range(10)}


class TestRename:
    def test_rename(self, r):
        r.set("a", "v", ex=100)
        assert r.rename("a", "b") is True
        assert r.get("b") == b"v"
        assert 0 < r.ttl("b") <= 100
        assert r.exists("a") == 0

    def test_rename_overwrites_dst(self, r):
        r.set("a", "v1")
        r.hset("b", "f", "v")
        r.rename("a", "b")
        assert r.get("b") == b"v1"

    def test_rename_missing_raises(self, r):
        with pytest.raises(ResponseError):
            r.rename("missing", "b")


class TestConcurrency:
    def test_concurrent_reads_during_write(self, r):
        r.set("k", "v")
        errors = []

        def reader():
            try:
                for _ in range(200):
                    assert r.get("k") is not None
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def writer():
            try:
                for i in range(200):
                    r.set("k", f"v{i}")
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        threads.append(threading.Thread(target=writer))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

    def test_pipeline_not_visible_to_other_threads_midway(self, r):
        # a pipeline's writes land atomically: either none or all
        done = threading.Event()
        seen = []

        def observer():
            while not done.is_set():
                a, b = r.mget("pa", "pb")
                seen.append((a is None, b is None))
        t = threading.Thread(target=observer)
        t.start()
        pipe = r.pipeline()
        pipe.set("pa", "1")
        pipe.set("pb", "2")
        pipe.execute()
        done.set()
        t.join()
        # never observe pa set while pb is missing
        assert (False, True) not in seen


class TestFixtures:
    def test_fixture_module_provides_clients(self, tmp_path):
        from babyredis.testing import babyredis_client  # noqa: F401
        from babyredis.testing import babyredis_client_decoded  # noqa: F401


class TestWrongType:
    def test_string_ops_on_hash(self, r):
        r.hset("h", "f", "v")
        for op in (lambda: r.get("h"), lambda: r.incr("h"),
                   lambda: r.append("h", "x"), lambda: r.strlen("h"),
                   lambda: r.getdel("h"),
                   lambda: r.set("h", "v", get=True)):
            with pytest.raises(ResponseError):
                op()

    def test_hash_ops_on_string(self, r):
        r.set("s", "v")
        for op in (lambda: r.hget("s", "f"), lambda: r.hset("s", "f", "v"),
                   lambda: r.hgetall("s"), lambda: r.hdel("s", "f"),
                   lambda: r.hlen("s"), lambda: r.hincrby("s", "f")):
            with pytest.raises(ResponseError):
                op()

    def test_set_overwrites_hash(self, r):
        r.hset("k", "f", "v")
        assert r.set("k", "now a string") is True
        assert r.get("k") == b"now a string"
        assert r.type("k") == b"string"

    def test_mget_skips_wrong_type(self, r):
        r.set("s", "v")
        r.hset("h", "f", "v")
        assert r.mget("s", "h") == [b"v", None]

    def test_delete_works_on_any_type(self, r):
        r.set("s", "v")
        r.hset("h", "f", "v")
        assert r.delete("s", "h") == 2
        assert r.dbsize() == 0


class TestServer:
    def test_dbsize_flushdb_ping(self, r):
        r.mset({"a": 1, "b": 2})
        assert r.dbsize() == 2
        assert r.ping() is True
        assert r.flushdb() is True
        assert r.dbsize() == 0

    def test_persistence_across_reopen(self, tmp_path):
        path = str(tmp_path / "persist.db")
        with BabyRedis(path) as r1:
            r1.set("k", "survives")
            r1.set("gone", "v", px=30)
        time.sleep(0.05)
        with BabyRedis(path) as r2:
            assert r2.get("k") == b"survives"
            assert r2.get("gone") is None

    def test_memory_mode(self):
        with BabyRedis(":memory:") as r:
            r.set("k", "v")
            assert r.get("k") == b"v"

    def test_redis_alias(self):
        assert Redis is BabyRedis

    def test_thread_safety(self, r):
        def worker():
            for _ in range(100):
                r.incr("counter")

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert r.get("counter") == b"400"

    def test_sweep_purges_expired_rows(self, tmp_path):
        r = BabyRedis(str(tmp_path / "sweep.db"), sweep_interval=0.0)
        r.set("dead", "v", px=20)
        time.sleep(0.05)
        r.set("alive", "v")  # any write triggers the sweep
        (count,) = r._get_conn().execute(
            "SELECT COUNT(*) FROM babyredis_keys"
        ).fetchone()
        assert count == 1
        r.close()

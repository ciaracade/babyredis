"""Compatibility suite ported from redka's Go tests (github.com/nalgeon/redka).

Each case mirrors a test in redka's internal/r{string,key,hash,set,zset,list}
db_test.go files, translated to redis-py semantics. Where redka deliberately
deviates from Redis, babyredis follows *Redis* and the divergence is noted in
a comment (marked "redka deviation").
"""

import time

import pytest

from babyredis import BabyRedis, DataError, ResponseError


@pytest.fixture
def r(tmp_path):
    client = BabyRedis(str(tmp_path / "compat.db"))
    yield client
    client.close()


class TestRedkaStrings:
    def test_set_value_coercion(self, r):
        r.set("zero", 0)
        r.set("age", 25)
        r.set("pi", 3.14)
        r.set("bytes", b"hello")
        r.set("empty", "")
        assert r.get("zero") == b"0"
        assert r.get("age") == b"25"
        assert r.get("pi") == b"3.14"
        assert r.get("bytes") == b"hello"
        assert r.get("empty") == b""

    def test_set_invalid_types(self, r):
        # redka coerces bool to "1"/"0"; redis-py (and we) reject it
        with pytest.raises(DataError):
            r.set("struct", {"name": "alice"})
        with pytest.raises(DataError):
            r.set("nil", None)
        with pytest.raises(DataError):
            r.set("bool", True)

    def test_get_missing_and_wrongtype(self, r):
        assert r.get("name") is None
        r.hset("person", "name", "alice")
        # redka deviation: redka's Get treats wrong-type keys as missing;
        # Redis raises WRONGTYPE
        with pytest.raises(ResponseError):
            r.get("person")

    def test_mget_partial_and_wrongtype(self, r):
        r.set("name", "alice")
        r.set("age", 25)
        assert r.mget("name", "age") == [b"alice", b"25"]
        assert r.mget("name", "key1") == [b"alice", None]
        assert r.mget("key1", "key2") == [None, None]
        r.hset("hash1", "f", "v")
        assert r.mget("hash1") == [None]  # MGET masks wrong-type as missing

    def test_set_overwrites_and_changes_type(self, r):
        r.set("name", "alice")
        assert r.set("name", "bob") is True
        assert r.get("name") == b"bob"
        # set over a hash succeeds in Redis (overwrites the key entirely)
        r.hset("person", "name", "alice")
        assert r.set("person", "now-a-string") is True
        assert r.get("person") == b"now-a-string"

    def test_set_xx_semantics(self, r):
        r.set("name", "alice")
        assert r.set("name", "bob", xx=True) is True
        assert r.get("name") == b"bob"
        assert r.set("missing", "v", xx=True) is None
        assert r.exists("missing") == 0
        r.set("name", "cindy", xx=True, ex=1)
        assert 0 < r.ttl("name") <= 1

    def test_set_nx_semantics(self, r):
        r.set("name", "alice")
        assert r.set("name", "bob", nx=True) is None
        assert r.get("name") == b"alice"
        assert r.set("city", "paris", nx=True, ex=1) is True
        assert 0 < r.ttl("city") <= 1

    def test_set_keepttl(self, r):
        r.set("name", "alice", ex=60)
        r.set("name", "bob")  # plain set clears TTL
        assert r.ttl("name") == -1
        r.set("name", "alice", ex=60)
        r.set("name", "bob", keepttl=True)
        assert 0 < r.ttl("name") <= 60

    def test_set_exat_in_the_past_expires_immediately(self, r):
        r.set("name", "alice", exat=time.time() - 60)
        assert r.get("name") is None
        assert r.exists("name") == 0

    def test_mset_atomic_rollback(self, r):
        # redka rolls back the whole SetMany batch on a bad value; so do we
        with pytest.raises(DataError):
            r.mset({"name": "alice", "bad": object()})
        assert r.get("name") is None

    def test_incr_cases(self, r):
        assert r.incrby("age", 25) == 25
        assert r.incrby("age", 10) == 35
        assert r.incrby("age", -10) == 25
        r.set("name", "alice")
        with pytest.raises(ResponseError):
            r.incrby("name", 1)
        r.hset("person", "f", "v")
        with pytest.raises(ResponseError):
            r.incrby("person", 10)

    def test_incrbyfloat_cases(self, r):
        assert r.incrbyfloat("pi", 3.14) == 3.14
        assert r.incrbyfloat("pi", 1.86) == 5.0
        assert r.get("pi") == b"5"  # Redis strips trailing .0
        assert r.incrbyfloat("pi", -1.5) == 3.5
        r.set("name", "alice")
        with pytest.raises(ResponseError):
            r.incrbyfloat("name", 1.5)


class TestRedkaKeys:
    def test_exists_count(self, r):
        r.set("name", "alice")
        r.set("age", 25)
        assert r.exists("name", "age") == 2
        assert r.exists("name", "key1") == 1
        assert r.exists("key1", "key2") == 0

    def test_delete_counts(self, r):
        r.set("name", "alice")
        r.set("age", 25)
        assert r.delete("name", "age") == 2
        r.set("name", "alice")
        r.set("age", 25)
        assert r.delete("name") == 1
        assert r.exists("age") == 1
        assert r.delete("key") == 0

    def test_flushdb(self, r):
        r.set("name", "alice")
        r.set("age", 25)
        r.flushdb()
        assert r.dbsize() == 0

    def test_expire_zero_expires_immediately(self, r):
        r.set("name", "alice")
        assert r.expire("name", 0) is True
        assert r.get("name") is None
        # the key slot is recycled cleanly
        r.set("name", "bob")
        assert r.get("name") == b"bob"
        assert r.ttl("name") == -1

    def test_expire_missing(self, r):
        assert r.expire("name", 10) is False
        assert r.expireat("name", time.time() + 10) is False
        assert r.persist("name") is False

    def test_dbsize(self, r):
        assert r.dbsize() == 0
        r.set("name", "alice")
        r.set("age", 25)
        assert r.dbsize() == 2

    def test_keys_patterns(self, r):
        r.set("name", "alice")
        r.set("age", 25)
        assert sorted(r.keys("*")) == [b"age", b"name"]
        assert r.keys("na*") == [b"name"]
        assert r.keys("key*") == []

    def test_randomkey(self, r):
        assert r.randomkey() is None
        r.set("name", "alice")
        r.set("age", 25)
        assert r.randomkey() in (b"name", b"age")

    def test_rename_basic_and_overwrite(self, r):
        r.set("name", "alice")
        assert r.rename("name", "key") is True
        assert r.exists("name") == 0
        assert r.get("key") == b"alice"
        r.set("age", 25)
        r.rename("key", "age")
        assert r.get("age") == b"alice"

    def test_rename_same_key_is_noop(self, r):
        r.set("name", "alice")
        assert r.rename("name", "name") is True
        assert r.get("name") == b"alice"

    def test_rename_missing_src(self, r):
        r.set("name", "alice")
        with pytest.raises(ResponseError):
            r.rename("key1", "name")
        assert r.get("name") == b"alice"

    def test_rename_across_types_allowed(self, r):
        # redka deviation: redka refuses cross-type rename with ErrKeyType;
        # Redis overwrites the destination regardless of type
        r.set("str", "alice")
        r.hset("hash", "f", "v")
        assert r.rename("str", "hash") is True
        assert r.get("hash") == b"alice"
        assert r.type("hash") == b"string"

    def test_renamenx(self, r):
        r.set("name", "alice")
        assert r.renamenx("name", "title") is True
        assert r.get("title") == b"alice"
        r.set("name", "bob")
        assert r.renamenx("name", "title") is False
        assert r.get("title") == b"alice"
        assert r.renamenx("title", "title") is False
        with pytest.raises(ResponseError):
            r.renamenx("missing", "x")

    def test_scan_pagination(self, r):
        for k in ("11", "12", "21", "22", "31"):
            r.set(k, "v")
        cursor, page1 = r.scan(0, count=2)
        assert page1 == [b"11", b"12"]
        cursor, page2 = r.scan(cursor, count=2)
        assert page2 == [b"21", b"22"]
        cursor, page3 = r.scan(cursor, count=2)
        assert page3 == [b"31"]
        assert set(r.scan_iter(match="2*")) == {b"21", b"22"}
        assert list(r.scan_iter(match="n*")) == []


class TestRedkaHashes:
    def test_hset_create_update(self, r):
        assert r.hset("person", "name", "alice") == 1
        assert r.hlen("person") == 1
        assert r.hset("person", "age", 25) == 1
        assert r.hlen("person") == 2
        assert r.hset("person", "name", "bob") == 0  # update, not create
        assert r.hget("person", "name") == b"bob"

    def test_hset_mapping_counts_new_only(self, r):
        r.hset("person", "name", "alice")
        assert r.hset("person", mapping={"name": "bob", "age": 50}) == 1
        assert r.hlen("person") == 2

    def test_hset_wrongtype(self, r):
        r.set("person", "alice")
        with pytest.raises(ResponseError):
            r.hset("person", "name", "alice")
        assert r.get("person") == b"alice"

    def test_hget_missing(self, r):
        r.hset("person", "name", "alice")
        assert r.hget("person", "age") is None
        assert r.hget("pet", "name") is None

    def test_hmget_partial(self, r):
        r.hset("person", mapping={"name": "alice", "age": 25})
        assert r.hmget("person", "name", "age") == [b"alice", b"25"]
        assert r.hmget("person", "name", "city") == [b"alice", None]
        assert r.hmget("pet", "name", "age") == [None, None]

    def test_hdel_counts(self, r):
        r.hset("person", mapping={"a": 1, "b": 2, "c": 3})
        assert r.hdel("person", "a", "b") == 2
        assert r.hlen("person") == 1
        assert r.hdel("person", "x", "y") == 0
        assert r.hdel("pet", "name") == 0

    def test_hdel_all_fields_deletes_key(self, r):
        # redka deviation: redka keeps the empty hash key; Redis deletes it
        r.hset("person", mapping={"a": 1, "b": 2})
        assert r.hdel("person", "a", "b") == 2
        assert r.exists("person") == 0

    def test_hdel_no_fields_raises(self, r):
        with pytest.raises(DataError):
            r.hdel("person")

    def test_hexists_hlen_hkeys_hvals_on_missing_and_wrongtype(self, r):
        assert r.hexists("pet", "name") is False
        assert r.hlen("robot") == 0
        assert r.hkeys("robot") == []
        assert r.hvals("robot") == []
        assert r.hgetall("robot") == {}
        r.set("str", "v")
        # redka deviation: redka reads on wrong type return empty; Redis
        # raises WRONGTYPE
        for op in (lambda: r.hexists("str", "f"), lambda: r.hlen("str"),
                   lambda: r.hkeys("str"), lambda: r.hvals("str"),
                   lambda: r.hgetall("str"), lambda: r.hget("str", "f")):
            with pytest.raises(ResponseError):
                op()

    def test_hincrby_cases(self, r):
        assert r.hincrby("person", "age", 25) == 25
        r.hset("person", "name", "alice")
        assert r.hincrby("person", "age", 10) == 35
        assert r.hincrby("person", "age", -10) == 25
        with pytest.raises(ResponseError):
            r.hincrby("person", "name", 10)
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.hincrby("str", "age", 25)

    def test_hincrbyfloat_cases(self, r):
        assert r.hincrbyfloat("person", "age", 25.5) == 25.5
        assert r.hincrbyfloat("person", "age", 10.5) == 36.0
        assert r.hincrbyfloat("person", "age", -10.5) == 25.5
        r.hset("person", "name", "alice")
        with pytest.raises(ResponseError):
            r.hincrbyfloat("person", "name", 10.5)

    def test_hsetnx_cases(self, r):
        assert r.hsetnx("person", "name", "alice") is True
        assert r.hsetnx("person", "age", 25) is True
        assert r.hsetnx("person", "name", "bob") is False
        assert r.hget("person", "name") == b"alice"

    def test_hscan_scoped_to_key(self, r):
        r.hset("person", "name", "alice")
        r.hset("pet", "name", "doggo")
        assert dict(r.hscan_iter("person")) == {b"name": b"alice"}


class TestRedkaSets:
    def test_sadd_counts(self, r):
        assert r.sadd("key", "one", "two", "thr") == 3
        assert r.sadd("key", "one", "two", "fou", "fiv") == 2
        assert r.scard("key") == 5

    def test_srem_counts(self, r):
        r.sadd("key", "one", "two", "thr")
        assert r.srem("key", "one", "two") == 2
        assert r.smembers("key") == {b"thr"}
        assert r.srem("key", "xxx", "yyy") == 0
        assert r.srem("nokey", "one") == 0

    def test_srem_all_deletes_key(self, r):
        # redka deviation: redka keeps the empty set key; Redis deletes it
        r.sadd("key", "one", "two")
        r.srem("key", "one", "two")
        assert r.exists("key") == 0

    def test_spop_last_deletes_key(self, r):
        # redka deviation: same as above
        r.sadd("key", "one")
        assert r.spop("key") == b"one"
        assert r.exists("key") == 0

    def test_spop_missing(self, r):
        assert r.spop("key") is None
        r.set("str", "v")
        with pytest.raises(ResponseError):  # redka deviation: redka NotFound
            r.spop("str")

    def test_srandmember_does_not_remove(self, r):
        r.sadd("key", "one", "two", "thr")
        assert r.srandmember("key") in (b"one", b"two", b"thr")
        assert r.scard("key") == 3
        assert r.srandmember("nokey") is None

    def test_smove_cases(self, r):
        r.sadd("src", "one", "two")
        r.sadd("dest", "thr", "fou")
        assert r.smove("src", "dest", "one") is True
        assert r.smembers("src") == {b"two"}
        assert r.smembers("dest") == {b"one", b"thr", b"fou"}
        # element not in src
        assert r.smove("src", "dest", "zzz") is False
        # src missing
        assert r.smove("nosrc", "dest", "one") is False
        # dest created on demand
        r.sadd("src2", "a")
        assert r.smove("src2", "newdest", "a") is True
        assert r.smembers("newdest") == {b"a"}
        # moving the last member deletes src (Redis; redka deviation)
        assert r.exists("src2") == 0
        # dest wrong type
        r.sadd("src3", "a")
        r.set("strdest", "v")
        with pytest.raises(ResponseError):
            r.smove("src3", "strdest", "a")

    def test_set_op_results(self, r):
        r.sadd("key1", "one", "two", "thr", "fiv")
        r.sadd("key2", "two", "thr", "fou")
        r.sadd("key3", "two", "thr")
        assert r.sdiff("key1", "key2", "key3") == {b"one", b"fiv"}
        assert r.sinter("key1", "key2", "key3") == {b"two", b"thr"}
        assert r.sunion("key2", "key3") == {b"two", b"thr", b"fou"}
        assert r.sdiff("key1") == {b"one", b"two", b"thr", b"fiv"}
        assert r.sdiff("missing", "key2") == set()
        assert r.sinter("key1", "missing") == set()
        assert r.sunion("missing1", "missing2") == set()

    def test_set_op_wrongtype_raises(self, r):
        # redka deviation: redka treats wrong-type sources as empty sets;
        # Redis raises WRONGTYPE
        r.sadd("key1", "one")
        r.set("str", "v")
        for op in (r.sinter, r.sunion, r.sdiff):
            with pytest.raises(ResponseError):
                op("key1", "str")

    def test_store_variants(self, r):
        r.sadd("key1", "one", "two", "thr", "fiv")
        r.sadd("key2", "two", "thr", "fou")
        assert r.sdiffstore("dest", "key1", "key2") == 2
        assert r.smembers("dest") == {b"one", b"fiv"}
        # store overwrites dest completely
        assert r.sinterstore("dest", "key1", "key2") == 2
        assert r.smembers("dest") == {b"two", b"thr"}
        assert r.sunionstore("dest", "key1", "key2") == 5
        assert r.scard("dest") == 5

    def test_store_empty_result_deletes_dest(self, r):
        r.sadd("dest", "old")
        r.sadd("key1", "a")
        assert r.sinterstore("dest", "key1", "missing") == 0
        assert r.exists("dest") == 0

    def test_sscan_scoped_and_paged(self, r):
        r.sadd("key", *(f"f{i}{j}" for i in (1, 2, 3) for j in (1, 2)))
        r.sadd("other", "f11")
        assert len(list(r.sscan_iter("key", count=2))) == 6
        assert set(r.sscan_iter("key", match="f2*")) == {b"f21", b"f22"}
        assert list(r.sscan_iter("nokey")) == []


class TestRedkaSortedSets:
    def test_zadd_created_vs_updated(self, r):
        assert r.zadd("key", {"one": 1, "two": 2, "thr": 3}) == 3
        assert r.zadd("key", {"one": 10, "two": 20, "fou": 4, "fiv": 5}) == 2
        assert r.zcard("key") == 5
        assert r.zscore("key", "one") == 10.0
        assert r.zscore("key", "thr") == 3.0

    def test_zadd_wrongtype(self, r):
        r.set("key", "str")
        with pytest.raises(ResponseError):
            r.zadd("key", {"one": 1})
        assert r.get("key") == b"str"

    def test_zcount_battery(self, r):
        r.zadd("key", {"one": 1, "two": 2, "2nd": 2, "thr": 3})
        assert r.zcount("key", 0, 0) == 0
        assert r.zcount("key", 1, 1) == 1
        assert r.zcount("key", 1, 2) == 3
        assert r.zcount("key", 1, 3) == 4
        assert r.zcount("key", 2, 2) == 2
        assert r.zcount("key", 3, 3) == 1
        assert r.zcount("key", 4, 4) == 0
        assert r.zcount("key", "-inf", 1) == 1
        assert r.zcount("key", 1, "+inf") == 4
        assert r.zcount("key", "-inf", "+inf") == 4
        assert r.zcount("nokey", 1, 2) == 0
        # redka deviation: redka returns 0 on wrong type; Redis raises
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.zcount("str", 1, 2)

    def test_zrem_counts_and_key_removal(self, r):
        r.zadd("key", {"one": 1, "two": 2, "thr": 3})
        assert r.zrem("key", "one", "two") == 2
        assert r.zcard("key") == 1
        assert r.zrem("key", "nope") == 0
        assert r.zrem("key", "thr") == 1
        assert r.exists("key") == 0
        assert r.zrem("nokey", "one") == 0

    def test_zrank_tie_breaking_lexicographic(self, r):
        r.zadd("key", {"one": 1, "two": 2, "thr": 3, "2nd": 2})
        assert r.zrank("key", "one") == 0
        assert r.zrank("key", "2nd") == 1  # "2nd" < "two" at score 2
        assert r.zrank("key", "two") == 2
        assert r.zrank("key", "thr") == 3
        assert r.zrevrank("key", "thr") == 0
        assert r.zrevrank("key", "two") == 1  # "two" > "2nd" descending
        assert r.zrevrank("key", "2nd") == 2
        assert r.zrevrank("key", "one") == 3
        assert r.zrank("key", "nope") is None
        assert r.zrank("nokey", "one") is None

    def test_zscore_missing(self, r):
        r.zadd("key", {"one": 1})
        assert r.zscore("key", "nope") is None
        assert r.zscore("nokey", "one") is None

    def test_zincrby_cases(self, r):
        assert r.zincrby("key", 25.5, "one") == 25.5
        assert r.zincrby("key", 10.5, "one") == 36.0
        assert r.zincrby("key", -10.5, "one") == 25.5
        assert r.zincrby("key", 25.5, "two") == 25.5
        assert r.zcard("key") == 2
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.zincrby("str", 1.0, "one")

    def test_zrange_with_ties(self, r):
        r.zadd("key", {"one": 1, "2nd": 2, "two": 2, "thr": 3})
        assert r.zrange("key", 0, 1) == [b"one", b"2nd"]
        assert r.zrange("key", 1, 2) == [b"2nd", b"two"]
        assert r.zrange("key", 4, 5) == []
        assert r.zrevrange("key", 0, 1) == [b"thr", b"two"]
        # negative indices work (redka deviation: redka returns empty)
        assert r.zrange("key", -2, -1) == [b"two", b"thr"]

    def test_zrangebyscore_with_ties_and_pagination(self, r):
        r.zadd("key", {"one": 10, "2nd": 20, "two": 20, "thr": 30})
        assert r.zrangebyscore("key", 0, 10) == [b"one"]
        assert r.zrangebyscore("key", 10, 20) == [b"one", b"2nd", b"two"]
        assert r.zrangebyscore("key", 20, 20) == [b"2nd", b"two"]
        assert r.zrangebyscore("key", 40, 50) == []
        assert r.zrevrangebyscore("key", 20, 10) == [b"two", b"2nd", b"one"]
        assert r.zrangebyscore("key", 10, 30, start=0, num=2) == [
            b"one", b"2nd"]
        assert r.zrangebyscore("key", 10, 30, start=1, num=2) == [
            b"2nd", b"two"]
        assert r.zrangebyscore("nokey", 0, 100) == []

    def test_zremrangebyrank(self, r):
        r.zadd("key", {"one": 1, "2nd": 2, "two": 2, "thr": 3})
        assert r.zremrangebyrank("key", 1, 2) == 2  # removes 2nd and two
        assert r.zrange("key", 0, -1) == [b"one", b"thr"]
        # negative indices work (redka deviation: redka returns 0)
        assert r.zremrangebyrank("key", -2, -1) == 2
        assert r.exists("key") == 0
        assert r.zremrangebyrank("nokey", 0, 1) == 0

    def test_zremrangebyscore(self, r):
        r.zadd("key", {"one": 1, "two": 2, "2nd": 2, "thr": 3})
        assert r.zremrangebyscore("key", 1, 2) == 3
        assert r.zrange("key", 0, -1) == [b"thr"]
        assert r.zremrangebyscore("key", "(3", "+inf") == 0
        assert r.zremrangebyscore("key", 3, 3) == 1
        assert r.exists("key") == 0

    def test_zinterstore_aggregations(self, r):
        r.zadd("key1", {"one": 1, "two": 2, "thr": 3})
        r.zadd("key2", {"two": 20, "thr": 3, "fou": 4})
        r.zadd("key3", {"two": 200, "thr": 3})
        assert r.zinterstore("dest", ["key1", "key2", "key3"]) == 2
        assert r.zrange("dest", 0, -1, withscores=True) == [
            (b"thr", 9.0), (b"two", 222.0)]
        r.zinterstore("dest", ["key1", "key2", "key3"], aggregate="MIN")
        assert r.zrange("dest", 0, -1, withscores=True) == [
            (b"two", 2.0), (b"thr", 3.0)]
        r.zinterstore("dest", ["key1", "key2", "key3"], aggregate="MAX")
        assert r.zrange("dest", 0, -1, withscores=True) == [
            (b"thr", 3.0), (b"two", 200.0)]

    def test_zinterstore_empty_result_deletes_dest(self, r):
        r.zadd("dest", {"old": 1})
        r.zadd("key1", {"a": 1})
        assert r.zinterstore("dest", ["key1", "missing"]) == 0
        assert r.exists("dest") == 0

    def test_zunionstore_with_weights_and_sets(self, r):
        r.zadd("key1", {"one": 1, "two": 2})
        r.zadd("key2", {"two": 20, "fou": 4})
        assert r.zunionstore("dest", ["key1", "key2"]) == 3
        assert r.zscore("dest", "two") == 22.0
        # dict argument = weights
        assert r.zunionstore("dest", {"key1": 10, "key2": 1}) == 3
        assert r.zscore("dest", "two") == 40.0
        # plain sets participate with score 1.0, like Redis
        r.sadd("tags", "one", "xyz")
        assert r.zunionstore("dest", ["key1", "tags"]) == 3
        assert r.zscore("dest", "one") == 2.0
        assert r.zscore("dest", "xyz") == 1.0
        # wrong-type source raises
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.zunionstore("dest", ["key1", "str"])

    def test_zscan_scoped(self, r):
        r.zadd("key", {f"f{i}": i for i in range(5)})
        r.zadd("other", {"f1": 99})
        items = dict(r.zscan_iter("key", count=2))
        assert len(items) == 5
        assert items[b"f1"] == 1.0


class TestRedkaLists:
    def test_push_returns_length(self, r):
        assert r.rpush("key", "elem") == 1
        assert r.rpush("key", "two") == 2
        assert r.rpush("key", "two") == 3  # duplicates allowed
        assert r.lpush("key2", "a") == 1
        assert r.lpush("key2", "b") == 2
        r.set("str", "v")
        with pytest.raises(ResponseError):
            r.rpush("str", "x")
        with pytest.raises(ResponseError):
            r.lpush("str", "x")

    def test_pop_sequences(self, r):
        r.rpush("key", "one", "two", "thr")
        assert r.rpop("key") == b"thr"
        assert r.rpop("key") == b"two"
        assert r.rpop("key") == b"one"
        assert r.exists("key") == 0
        r.rpush("key", "one", "two", "thr")
        assert r.lpop("key") == b"one"
        assert r.lpop("key") == b"two"
        assert r.lpop("key") == b"thr"
        assert r.lpop("nokey") is None
        assert r.rpop("nokey") is None

    def test_lindex_cases(self, r):
        r.rpush("key", "one", "two", "thr")
        assert r.lindex("key", 0) == b"one"
        assert r.lindex("key", 1) == b"two"
        assert r.lindex("key", 2) == b"thr"
        assert r.lindex("key", -2) == b"two"
        assert r.lindex("key", 3) is None
        assert r.lindex("nokey", 0) is None

    def test_linsert_cases(self, r):
        r.rpush("key", "mark")
        assert r.linsert("key", "after", "mark", "elem") == 2
        assert r.lrange("key", 0, -1) == [b"mark", b"elem"]
        r.delete("key")
        r.rpush("key", "one", "thr")
        assert r.linsert("key", "after", "one", "two") == 3
        assert r.lrange("key", 0, -1) == [b"one", b"two", b"thr"]
        r.delete("key")
        r.rpush("key", "mark")
        assert r.linsert("key", "before", "mark", "elem") == 2
        assert r.lrange("key", 0, -1) == [b"elem", b"mark"]
        # pivot missing -> -1, list unchanged
        assert r.linsert("key", "before", "nope", "x") == -1
        assert r.llen("key") == 2
        # key missing -> 0
        assert r.linsert("nokey", "before", "a", "b") == 0

    def test_lrem_all_occurrences(self, r):
        r.rpush("key", "one", "two", "two", "thr", "two", "fou")
        assert r.lrem("key", 0, "two") == 3
        assert r.lrange("key", 0, -1) == [b"one", b"thr", b"fou"]
        assert r.lrem("key", 0, "nope") == 0
        assert r.lrem("nokey", 0, "x") == 0

    def test_lrem_from_front_and_back(self, r):
        r.rpush("key", "one", "two", "two", "thr", "two", "fou")
        assert r.lrem("key", 2, "two") == 2  # first two occurrences
        assert r.lrange("key", 0, -1) == [b"one", b"thr", b"two", b"fou"]
        r.delete("key")
        r.rpush("key", "one", "two", "two", "thr", "two", "fou")
        assert r.lrem("key", -2, "two") == 2  # last two occurrences
        assert r.lrange("key", 0, -1) == [b"one", b"two", b"thr", b"fou"]
        # count > occurrences removes all
        r.delete("key")
        r.rpush("key", "one", "two", "thr", "two", "fou")
        assert r.lrem("key", 10, "two") == 2
        assert r.llen("key") == 3

    def test_lrange_battery(self, r):
        r.rpush("key", "one", "two", "thr")
        assert r.lrange("key", 0, 1) == [b"one", b"two"]
        assert r.lrange("key", 2, 2) == [b"thr"]
        assert r.lrange("key", 3, 3) == []          # start >= len
        assert r.lrange("key", 1, 0) == []          # start > stop
        assert r.lrange("key", -1, -2) == []        # inverted negatives
        assert r.lrange("key", 1, 5) == [b"two", b"thr"]   # stop clamped
        assert r.lrange("key", -2, 2) == [b"two", b"thr"]
        assert r.lrange("key", 1, -1) == [b"two", b"thr"]
        assert r.lrange("key", -2, -1) == [b"two", b"thr"]
        assert r.lrange("nokey", 0, 0) == []

    def test_lset_cases(self, r):
        r.rpush("key", "one", "two", "two", "thr")
        assert r.lset("key", 1, "new") is True
        assert r.lrange("key", 0, -1) == [b"one", b"new", b"two", b"thr"]
        assert r.lset("key", -2, "neg") is True
        assert r.lindex("key", 2) == b"neg"
        with pytest.raises(ResponseError):
            r.lset("key", 9, "x")
        with pytest.raises(ResponseError):
            r.lset("nokey", 0, "x")

    def test_ltrim_battery(self, r):
        r.rpush("key", "one", "two", "thr", "fou")
        r.ltrim("key", 1, 2)
        assert r.lrange("key", 0, -1) == [b"two", b"thr"]
        r.delete("key")
        r.rpush("key", "one", "two", "thr")
        r.ltrim("key", 0, 5)  # stop clamped, keeps all
        assert r.llen("key") == 3
        r.ltrim("key", 3, 3)  # start >= len: everything removed, key gone
        assert r.exists("key") == 0
        r.rpush("key", "one", "two", "thr")
        r.ltrim("key", 2, 1)  # start > stop: everything removed
        assert r.exists("key") == 0
        r.rpush("key", "one", "two", "thr")
        r.ltrim("key", -2, -1)
        assert r.lrange("key", 0, -1) == [b"two", b"thr"]
        assert r.ltrim("nokey", 0, 0) is True

    def test_rpoplpush_basic(self, r):
        r.rpush("src", "one", "two", "thr")
        assert r.rpoplpush("src", "dest") == b"thr"
        assert r.rpoplpush("src", "dest") == b"two"
        assert r.rpoplpush("src", "dest") == b"one"
        assert r.lrange("dest", 0, -1) == [b"one", b"two", b"thr"]
        assert r.exists("src") == 0
        # missing src -> None (Redis returns nil; redka raises NotFound)
        assert r.rpoplpush("nokey", "dest") is None

    def test_rpoplpush_same_key_rotation(self, r):
        r.rpush("key", "one", "two", "thr")
        assert r.rpoplpush("key", "key") == b"thr"
        assert r.lrange("key", 0, -1) == [b"thr", b"one", b"two"]
        assert r.rpoplpush("key", "key") == b"two"
        assert r.lrange("key", 0, -1) == [b"two", b"thr", b"one"]

    def test_lmove_directions(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lmove("a", "b", "LEFT", "RIGHT") == b"1"
        assert r.lmove("a", "b", "RIGHT", "LEFT") == b"3"
        assert r.lrange("a", 0, -1) == [b"2"]
        assert r.lrange("b", 0, -1) == [b"3", b"1"]
        with pytest.raises(DataError):
            r.lmove("a", "b", "SIDEWAYS", "LEFT")
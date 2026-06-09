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
        (count,) = r._conn.execute(
            "SELECT COUNT(*) FROM babyredis_keys"
        ).fetchone()
        assert count == 1
        r.close()

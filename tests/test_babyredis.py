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
            "SELECT COUNT(*) FROM babyredis_kv"
        ).fetchone()
        assert count == 1
        r.close()

"""Property-based compatibility tests with fakeredis as the oracle.

A Hypothesis state machine fires random command sequences at both
fakeredis (which models real Redis) and babyredis, asserting after every
command that both return the same result or both fail, and that dbsize
stays in lockstep. This catches interaction bugs that hand-written cases
miss.

Deliberately excluded for determinism: TTL commands (time-dependent),
spop/srandmember/randomkey (random), and scan cursors (cursor values are
implementation-defined; iteration is covered in the unit tests).
"""

import shutil
import tempfile

import pytest

pytest.importorskip("hypothesis")
pytest.importorskip("fakeredis")

import fakeredis
import redis.exceptions
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from babyredis import BabyRedis, BabyRedisError

# small pools so keys collide across types and WRONGTYPE paths get exercised
keys_st = st.sampled_from([f"k{i}" for i in range(5)])
fields_st = st.sampled_from([f"f{i}" for i in range(4)])
members_st = st.sampled_from([f"m{i}" for i in range(4)])
values_st = st.one_of(
    st.sampled_from(["", "alice", "42", "héllo"]),
    st.integers(-100, 100),
    st.binary(max_size=8),
)
# exact binary fractions only, so float string formatting matches Redis
scores_st = st.one_of(st.integers(-5, 5),
                      st.sampled_from([0.5, -1.5, 2.5, -0.25]))
index_st = st.integers(-6, 6)


def normalize(value):
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    return value


class OracleMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.tmp = tempfile.mkdtemp()
        self.baby = BabyRedis(f"{self.tmp}/oracle.db")
        self.real = fakeredis.FakeRedis()

    def teardown(self):
        self.baby.close()
        self.real.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def compare(self, method, *args, **kwargs):
        try:
            expected = getattr(self.real, method)(*args, **kwargs)
            real_err = None
        except redis.exceptions.RedisError as exc:
            expected, real_err = None, exc
        try:
            actual = getattr(self.baby, method)(*args, **kwargs)
            baby_err = None
        except BabyRedisError as exc:
            actual, baby_err = None, exc
        assert (real_err is None) == (baby_err is None), (
            f"{method} {args} {kwargs}: fakeredis={real_err!r}"
            f" babyredis={baby_err!r}")
        if real_err is None:
            assert normalize(actual) == normalize(expected), (
                f"{method} {args} {kwargs}: fakeredis={expected!r}"
                f" babyredis={actual!r}")

    @invariant()
    def dbsize_matches(self):
        assert self.baby.dbsize() == self.real.dbsize()

    # -- strings --------------------------------------------------------

    @rule(key=keys_st, value=values_st,
          flag=st.sampled_from([None, "nx", "xx", "get"]))
    def set(self, key, value, flag):
        kwargs = {flag: True} if flag else {}
        self.compare("set", key, value, **kwargs)

    @rule(key=keys_st)
    def get(self, key):
        self.compare("get", key)

    @rule(key=keys_st)
    def getdel(self, key):
        self.compare("getdel", key)

    @rule(key=keys_st, value=values_st)
    def getset(self, key, value):
        self.compare("getset", key, value)

    @rule(key=keys_st, value=values_st)
    def append(self, key, value):
        self.compare("append", key, value)

    @rule(key=keys_st)
    def strlen(self, key):
        self.compare("strlen", key)

    @rule(key=keys_st, amount=st.integers(-100, 100))
    def incrby(self, key, amount):
        self.compare("incrby", key, amount)

    @rule(key=keys_st, amount=scores_st)
    def incrbyfloat(self, key, amount):
        self.compare("incrbyfloat", key, float(amount))

    @rule(k1=keys_st, k2=keys_st, v1=values_st, v2=values_st)
    def mset(self, k1, k2, v1, v2):
        self.compare("mset", {k1: v1, k2: v2})

    @rule(k1=keys_st, k2=keys_st)
    def mget(self, k1, k2):
        self.compare("mget", k1, k2)

    # -- keys -----------------------------------------------------------

    @rule(k1=keys_st, k2=keys_st)
    def delete(self, k1, k2):
        self.compare("delete", k1, k2)

    @rule(k1=keys_st, k2=keys_st)
    def exists(self, k1, k2):
        self.compare("exists", k1, k2)

    @rule(key=keys_st)
    def type(self, key):
        self.compare("type", key)

    @rule(src=keys_st, dst=keys_st)
    def rename(self, src, dst):
        self.compare("rename", src, dst)

    @rule(src=keys_st, dst=keys_st)
    def renamenx(self, src, dst):
        self.compare("renamenx", src, dst)

    @rule()
    def keys_match(self):
        assert sorted(self.baby.keys()) == sorted(self.real.keys())

    # -- hashes ---------------------------------------------------------

    @rule(key=keys_st, field=fields_st, value=values_st)
    def hset(self, key, field, value):
        self.compare("hset", key, field, value)

    @rule(key=keys_st, f1=fields_st, f2=fields_st, v1=values_st,
          v2=values_st)
    def hset_mapping(self, key, f1, f2, v1, v2):
        self.compare("hset", key, mapping={f1: v1, f2: v2})

    @rule(key=keys_st, field=fields_st)
    def hget(self, key, field):
        self.compare("hget", key, field)

    @rule(key=keys_st)
    def hgetall(self, key):
        self.compare("hgetall", key)

    @rule(key=keys_st, f1=fields_st, f2=fields_st)
    def hdel(self, key, f1, f2):
        self.compare("hdel", key, f1, f2)

    @rule(key=keys_st, field=fields_st)
    def hexists(self, key, field):
        self.compare("hexists", key, field)

    @rule(key=keys_st)
    def hlen(self, key):
        self.compare("hlen", key)

    @rule(key=keys_st, f1=fields_st, f2=fields_st)
    def hmget(self, key, f1, f2):
        self.compare("hmget", key, f1, f2)

    @rule(key=keys_st, field=fields_st, value=values_st)
    def hsetnx(self, key, field, value):
        self.compare("hsetnx", key, field, value)

    @rule(key=keys_st, field=fields_st, amount=st.integers(-100, 100))
    def hincrby(self, key, field, amount):
        self.compare("hincrby", key, field, amount)

    # -- sets -----------------------------------------------------------

    @rule(key=keys_st, m1=members_st, m2=members_st)
    def sadd(self, key, m1, m2):
        self.compare("sadd", key, m1, m2)

    @rule(key=keys_st, m1=members_st, m2=members_st)
    def srem(self, key, m1, m2):
        self.compare("srem", key, m1, m2)

    @rule(key=keys_st)
    def smembers(self, key):
        self.compare("smembers", key)

    @rule(key=keys_st, member=members_st)
    def sismember(self, key, member):
        self.compare("sismember", key, member)

    @rule(key=keys_st)
    def scard(self, key):
        self.compare("scard", key)

    @rule(src=keys_st, dst=keys_st, member=members_st)
    def smove(self, src, dst, member):
        self.compare("smove", src, dst, member)

    @rule(k1=keys_st, k2=keys_st,
          op=st.sampled_from(["sinter", "sunion", "sdiff"]))
    def set_ops(self, k1, k2, op):
        self.compare(op, k1, k2)

    # -- sorted sets ----------------------------------------------------

    @rule(key=keys_st, m1=members_st, m2=members_st, s1=scores_st,
          s2=scores_st, flags=st.sampled_from([{}, {"nx": True},
                                               {"xx": True}, {"ch": True}]))
    def zadd(self, key, m1, m2, s1, s2, flags):
        self.compare("zadd", key, {m1: s1, m2: s2}, **flags)

    @rule(key=keys_st, member=members_st)
    def zscore(self, key, member):
        self.compare("zscore", key, member)

    @rule(key=keys_st, m1=members_st, m2=members_st)
    def zrem(self, key, m1, m2):
        self.compare("zrem", key, m1, m2)

    @rule(key=keys_st)
    def zcard(self, key):
        self.compare("zcard", key)

    @rule(key=keys_st, lo=st.integers(-5, 5), hi=st.integers(-5, 5))
    def zcount(self, key, lo, hi):
        self.compare("zcount", key, lo, hi)

    @rule(key=keys_st, start=index_st, end=index_st,
          withscores=st.booleans())
    def zrange(self, key, start, end, withscores):
        self.compare("zrange", key, start, end, withscores=withscores)

    @rule(key=keys_st, start=index_st, end=index_st)
    def zrevrange(self, key, start, end):
        self.compare("zrevrange", key, start, end)

    @rule(key=keys_st, lo=st.integers(-5, 5), hi=st.integers(-5, 5),
          withscores=st.booleans())
    def zrangebyscore(self, key, lo, hi, withscores):
        self.compare("zrangebyscore", key, lo, hi, withscores=withscores)

    @rule(key=keys_st, member=members_st)
    def zrank(self, key, member):
        self.compare("zrank", key, member)

    @rule(key=keys_st, member=members_st)
    def zrevrank(self, key, member):
        self.compare("zrevrank", key, member)

    @rule(key=keys_st, amount=scores_st, member=members_st)
    def zincrby(self, key, amount, member):
        self.compare("zincrby", key, float(amount), member)

    @rule(key=keys_st, count=st.integers(0, 3),
          op=st.sampled_from(["zpopmin", "zpopmax"]))
    def zpop(self, key, count, op):
        self.compare(op, key, count)
        # fakeredis 2.36 bug: a count'ed zpop that empties the zset leaves
        # a ghost key (visible to keys/dbsize/type but not exists); real
        # Redis deletes the key. Rehydrate-and-delete to resync the oracle.
        if self.real.exists(key) == 0 and key.encode() in self.real.keys():
            self.real.zadd(key, {"__ghost__": 0})
            self.real.delete(key)

    @rule(key=keys_st, start=index_st, end=index_st)
    def zremrangebyrank(self, key, start, end):
        self.compare("zremrangebyrank", key, start, end)

    @rule(key=keys_st, lo=st.integers(-5, 5), hi=st.integers(-5, 5))
    def zremrangebyscore(self, key, lo, hi):
        self.compare("zremrangebyscore", key, lo, hi)

    # -- lists ----------------------------------------------------------

    @rule(key=keys_st, v1=values_st, v2=values_st,
          op=st.sampled_from(["lpush", "rpush"]))
    def push(self, key, v1, v2, op):
        self.compare(op, key, v1, v2)

    @rule(key=keys_st, count=st.sampled_from([None, 0, 1, 2]),
          op=st.sampled_from(["lpop", "rpop"]))
    def pop(self, key, count, op):
        self.compare(op, key, count)

    @rule(key=keys_st)
    def llen(self, key):
        self.compare("llen", key)

    @rule(key=keys_st, start=index_st, end=index_st)
    def lrange(self, key, start, end):
        self.compare("lrange", key, start, end)

    @rule(key=keys_st, index=index_st)
    def lindex(self, key, index):
        self.compare("lindex", key, index)

    @rule(key=keys_st, index=index_st, value=values_st)
    def lset(self, key, index, value):
        self.compare("lset", key, index, value)

    @rule(key=keys_st, count=st.integers(-2, 2), value=values_st)
    def lrem(self, key, count, value):
        self.compare("lrem", key, count, value)

    @rule(key=keys_st, start=index_st, end=index_st)
    def ltrim(self, key, start, end):
        self.compare("ltrim", key, start, end)

    @rule(key=keys_st, where=st.sampled_from(["before", "after"]),
          ref=values_st, value=values_st)
    def linsert(self, key, where, ref, value):
        self.compare("linsert", key, where, ref, value)

    @rule(src=keys_st, dst=keys_st)
    def rpoplpush(self, src, dst):
        self.compare("rpoplpush", src, dst)


TestOracle = OracleMachine.TestCase
TestOracle.settings = settings(
    max_examples=40, stateful_step_count=60, deadline=None)

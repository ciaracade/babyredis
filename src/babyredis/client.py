"""SQLite-backed key-value store with a redis-py compatible API.

No server process: data lives in a single SQLite file (WAL mode) so it
persists across restarts and can be shared between processes on the same
machine. Pass ``":memory:"`` for an ephemeral, single-process store.
"""

import math
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import timedelta


class BabyRedisError(Exception):
    """Base error for babyredis."""


class DataError(BabyRedisError, ValueError):
    """Raised when a value cannot be stored (mirrors redis.exceptions.DataError)."""


class ResponseError(BabyRedisError):
    """Raised when an operation is invalid for the stored value
    (mirrors redis.exceptions.ResponseError)."""


def _glob_to_regex(pattern):
    """Translate a Redis glob pattern (*, ?, [...], \\escape) to a regex."""
    i, n = 0, len(pattern)
    out = []
    while i < n:
        ch = pattern[i]
        i += 1
        if ch == "\\" and i < n:
            out.append(re.escape(pattern[i]))
            i += 1
        elif ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        elif ch == "[":
            j = i
            negate = j < n and pattern[j] in "^!"
            if negate:
                j += 1
            body = []
            while j < n and pattern[j] != "]":
                if pattern[j] == "\\" and j + 1 < n:
                    j += 1
                body.append(pattern[j])
                j += 1
            if j >= n:  # unterminated bracket: literal '['
                out.append(re.escape(ch))
            else:
                inner = "".join(body).replace("\\", "\\\\").replace("]", "\\]")
                out.append("[" + ("^" if negate else "") + inner + "]")
                i = j + 1
        else:
            out.append(re.escape(ch))
    return re.compile("(?s)^" + "".join(out) + "$")


def _to_seconds(value, name):
    if isinstance(value, timedelta):
        value = value.total_seconds()
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DataError(f"{name} must be an int, float, or timedelta")
    return float(value)


class BabyRedis:
    """A redis-py shaped client backed by SQLite.

    Args:
        path: SQLite database file, or ":memory:" for an ephemeral store.
        decode_responses: return ``str`` instead of ``bytes`` (same flag as
            redis-py).
        sweep_interval: minimum seconds between background purges of expired
            keys. Expired keys are always invisible regardless of sweeping.
    """

    def __init__(self, path="babyredis.db", *, decode_responses=False,
                 sweep_interval=60.0):
        self.decode_responses = decode_responses
        self._lock = threading.RLock()
        self._sweep_interval = sweep_interval
        self._last_sweep = 0.0
        self._conn = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False,
            timeout=30.0,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS babyredis_kv ("
            " key TEXT PRIMARY KEY,"
            " value BLOB NOT NULL,"
            " expires_at REAL"
            ") WITHOUT ROWID"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS babyredis_kv_expires"
            " ON babyredis_kv(expires_at) WHERE expires_at IS NOT NULL"
        )

    # -- plumbing ---------------------------------------------------------

    @contextmanager
    def _tx_block(self):
        """Run statements in a single write transaction (caller holds the lock)."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    @staticmethod
    def _encode_key(name):
        if isinstance(name, bytes):
            return name.decode("utf-8")
        if isinstance(name, str):
            return name
        raise DataError("keys must be str or bytes")

    @staticmethod
    def _encode_value(value):
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value).encode("ascii")
        raise DataError(
            f"invalid value type {type(value).__name__!r}: babyredis accepts"
            " bytes, str, int, or float"
        )

    def _decode(self, value):
        if value is None:
            return None
        value = bytes(value)
        return value.decode("utf-8") if self.decode_responses else value

    def _maybe_sweep(self, now):
        if now - self._last_sweep >= self._sweep_interval:
            self._last_sweep = now
            self._conn.execute(
                "DELETE FROM babyredis_kv WHERE expires_at IS NOT NULL"
                " AND expires_at <= ?", (now,)
            )

    def _load(self, key, now):
        """Return (value, expires_at) honoring expiry, or None if missing."""
        row = self._conn.execute(
            "SELECT value, expires_at FROM babyredis_kv WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        if row[1] is not None and row[1] <= now:
            self._conn.execute("DELETE FROM babyredis_kv WHERE key = ?", (key,))
            return None
        return row

    # -- strings ----------------------------------------------------------

    def set(self, name, value, ex=None, px=None, nx=False, xx=False,
            keepttl=False, get=False, exat=None, pxat=None):
        key = self._encode_key(name)
        data = self._encode_value(value)
        now = time.time()

        expires_at = None
        if ex is not None:
            expires_at = now + _to_seconds(ex, "ex")
        elif px is not None:
            expires_at = now + _to_seconds(px, "px") / 1000.0
        elif exat is not None:
            expires_at = _to_seconds(exat, "exat")
        elif pxat is not None:
            expires_at = _to_seconds(pxat, "pxat") / 1000.0

        with self._lock, self._tx_block():
            self._maybe_sweep(now)
            row = self._load(key, now)
            old = row[0] if row else None
            should_write = not ((nx and row is not None) or (xx and row is None))
            if should_write:
                if keepttl and row is not None:
                    expires_at = row[1]
                self._conn.execute(
                    "INSERT INTO babyredis_kv (key, value, expires_at)"
                    " VALUES (?, ?, ?)"
                    " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                    " expires_at=excluded.expires_at",
                    (key, data, expires_at),
                )
        if get:
            return self._decode(old)
        return True if should_write else None

    def get(self, name):
        key = self._encode_key(name)
        with self._lock:
            row = self._load(key, time.time())
        return self._decode(row[0]) if row else None

    def getdel(self, name):
        key = self._encode_key(name)
        with self._lock, self._tx_block():
            row = self._load(key, time.time())
            if row:
                self._conn.execute(
                    "DELETE FROM babyredis_kv WHERE key = ?", (key,)
                )
        return self._decode(row[0]) if row else None

    def getset(self, name, value):
        # Like Redis GETSET: sets the new value and clears any TTL.
        return self.set(name, value, get=True)

    def setex(self, name, time, value):
        return self.set(name, value, ex=time)

    def psetex(self, name, time_ms, value):
        return self.set(name, value, px=time_ms)

    def setnx(self, name, value):
        return self.set(name, value, nx=True) is True

    def append(self, name, value):
        key = self._encode_key(name)
        data = self._encode_value(value)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._load(key, now)
            new = (bytes(row[0]) if row else b"") + data
            self._conn.execute(
                "INSERT INTO babyredis_kv (key, value, expires_at)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, new, row[1] if row else None),
            )
        return len(new)

    def strlen(self, name):
        key = self._encode_key(name)
        with self._lock:
            row = self._load(key, time.time())
        return len(bytes(row[0])) if row else 0

    def mset(self, mapping):
        now = time.time()
        items = [(self._encode_key(k), self._encode_value(v))
                 for k, v in mapping.items()]
        with self._lock, self._tx_block():
            self._maybe_sweep(now)
            for key, data in items:
                self._conn.execute(
                    "INSERT INTO babyredis_kv (key, value, expires_at)"
                    " VALUES (?, ?, NULL)"
                    " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                    " expires_at=NULL",
                    (key, data),
                )
        return True

    def mget(self, keys, *args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        names = list(keys) + list(args)
        now = time.time()
        out = []
        with self._lock:
            for name in names:
                row = self._load(self._encode_key(name), now)
                out.append(self._decode(row[0]) if row else None)
        return out

    # -- counters ---------------------------------------------------------

    def incrby(self, name, amount=1):
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise DataError("amount must be an int")
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._load(key, now)
            if row is None:
                current, expires_at = 0, None
            else:
                try:
                    current = int(bytes(row[0]))
                except ValueError:
                    raise ResponseError(
                        "value is not an integer or out of range"
                    ) from None
                expires_at = row[1]
            new = current + amount
            self._conn.execute(
                "INSERT INTO babyredis_kv (key, value, expires_at)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(new).encode("ascii"), expires_at),
            )
        return new

    incr = incrby

    def decrby(self, name, amount=1):
        return self.incrby(name, -amount)

    decr = decrby

    # -- keys -------------------------------------------------------------

    def delete(self, *names):
        now = time.time()
        count = 0
        with self._lock, self._tx_block():
            for name in names:
                key = self._encode_key(name)
                if self._load(key, now) is not None:
                    self._conn.execute(
                        "DELETE FROM babyredis_kv WHERE key = ?", (key,)
                    )
                    count += 1
        return count

    def exists(self, *names):
        now = time.time()
        count = 0
        with self._lock:
            for name in names:
                if self._load(self._encode_key(name), now) is not None:
                    count += 1
        return count

    def expire(self, name, time_):
        key = self._encode_key(name)
        now = time.time()
        seconds = _to_seconds(time_, "time")
        with self._lock, self._tx_block():
            if self._load(key, now) is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_kv SET expires_at = ? WHERE key = ?",
                (now + seconds, key),
            )
        return True

    def pexpire(self, name, time_ms):
        return self.expire(name, _to_seconds(time_ms, "time") / 1000.0)

    def expireat(self, name, when):
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            if self._load(key, now) is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_kv SET expires_at = ? WHERE key = ?",
                (_to_seconds(when, "when"), key),
            )
        return True

    def persist(self, name):
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._load(key, now)
            if row is None or row[1] is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_kv SET expires_at = NULL WHERE key = ?",
                (key,),
            )
        return True

    def ttl(self, name):
        pttl = self.pttl(name)
        return pttl if pttl < 0 else max(1, math.ceil(pttl / 1000.0))

    def pttl(self, name):
        key = self._encode_key(name)
        now = time.time()
        with self._lock:
            row = self._load(key, now)
        if row is None:
            return -2
        if row[1] is None:
            return -1
        return max(1, math.ceil((row[1] - now) * 1000.0))

    def keys(self, pattern="*"):
        if isinstance(pattern, bytes):
            pattern = pattern.decode("utf-8")
        regex = _glob_to_regex(pattern)
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM babyredis_kv WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchall()
        matched = [k for (k,) in rows if regex.match(k)]
        if self.decode_responses:
            return matched
        return [k.encode("utf-8") for k in matched]

    def type(self, name):
        result = "string" if self.exists(name) else "none"
        return result if self.decode_responses else result.encode("ascii")

    # -- server-ish -------------------------------------------------------

    def dbsize(self):
        now = time.time()
        with self._lock:
            (count,) = self._conn.execute(
                "SELECT COUNT(*) FROM babyredis_kv WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchone()
        return count

    def flushdb(self):
        with self._lock:
            self._conn.execute("DELETE FROM babyredis_kv")
        return True

    flushall = flushdb

    def ping(self):
        return True

    def close(self):
        with self._lock:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# Drop-in friendly alias: `import babyredis; r = babyredis.Redis()`
Redis = BabyRedis
StrictRedis = BabyRedis

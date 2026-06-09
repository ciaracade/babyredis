"""SQLite-backed key-value store with a redis-py compatible API.

No server process: data lives in a single SQLite file (WAL mode) so it
persists across restarts and can be shared between processes on the same
machine. Pass ``":memory:"`` for an ephemeral, single-process store.

Schema: a master ``babyredis_keys`` table owns the key name, its type, and
its TTL; one child table per data type holds the payload and cascades on
delete. This keeps expiry and DEL in one place and lets type mismatches
fail with WRONGTYPE like real Redis.
"""

import math
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import timedelta

_WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


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


_SCHEMA = """
CREATE TABLE IF NOT EXISTS babyredis_keys (
  id INTEGER PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  expires_at REAL
);
CREATE INDEX IF NOT EXISTS babyredis_keys_expires
  ON babyredis_keys(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS babyredis_strings (
  key_id INTEGER PRIMARY KEY
    REFERENCES babyredis_keys(id) ON DELETE CASCADE,
  value BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS babyredis_hashes (
  key_id INTEGER NOT NULL
    REFERENCES babyredis_keys(id) ON DELETE CASCADE,
  field TEXT NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (key_id, field)
) WITHOUT ROWID;
"""


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
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)

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

    def _decode_field(self, field):
        return field if self.decode_responses else field.encode("utf-8")

    def _maybe_sweep(self, now):
        if now - self._last_sweep >= self._sweep_interval:
            self._last_sweep = now
            self._conn.execute(
                "DELETE FROM babyredis_keys WHERE expires_at IS NOT NULL"
                " AND expires_at <= ?", (now,)
            )

    def _lookup(self, key, now):
        """Return (id, type, expires_at) honoring expiry, or None if missing."""
        row = self._conn.execute(
            "SELECT id, type, expires_at FROM babyredis_keys WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        if row[2] is not None and row[2] <= now:
            self._conn.execute(
                "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
            )
            return None
        return row

    def _typed_lookup(self, key, now, expected):
        row = self._lookup(key, now)
        if row is not None and row[1] != expected:
            raise ResponseError(_WRONGTYPE)
        return row

    def _create_key(self, key, type_, expires_at):
        cur = self._conn.execute(
            "INSERT INTO babyredis_keys (key, type, expires_at)"
            " VALUES (?, ?, ?)",
            (key, type_, expires_at),
        )
        return cur.lastrowid

    def _string_value(self, key_id):
        (value,) = self._conn.execute(
            "SELECT value FROM babyredis_strings WHERE key_id = ?", (key_id,)
        ).fetchone()
        return bytes(value)

    def _put_string(self, key, row, data, expires_at):
        """Write a string payload, reusing the key row when types match."""
        if row is not None and row[1] == "string":
            self._conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (expires_at, row[0]),
            )
            self._conn.execute(
                "UPDATE babyredis_strings SET value = ? WHERE key_id = ?",
                (data, row[0]),
            )
        else:
            if row is not None:
                self._conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                )
            key_id = self._create_key(key, "string", expires_at)
            self._conn.execute(
                "INSERT INTO babyredis_strings (key_id, value) VALUES (?, ?)",
                (key_id, data),
            )

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
            row = self._lookup(key, now)
            if get and row is not None and row[1] != "string":
                raise ResponseError(_WRONGTYPE)
            old = self._string_value(row[0]) \
                if row is not None and row[1] == "string" else None
            should_write = not ((nx and row is not None) or (xx and row is None))
            if should_write:
                if keepttl and row is not None:
                    expires_at = row[2]
                self._put_string(key, row, data, expires_at)
        if get:
            return self._decode(old)
        return True if should_write else None

    def get(self, name):
        key = self._encode_key(name)
        with self._lock:
            row = self._typed_lookup(key, time.time(), "string")
            return self._decode(self._string_value(row[0])) if row else None

    def getdel(self, name):
        key = self._encode_key(name)
        with self._lock, self._tx_block():
            row = self._typed_lookup(key, time.time(), "string")
            if row is None:
                return None
            old = self._string_value(row[0])
            self._conn.execute(
                "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
            )
        return self._decode(old)

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
            row = self._typed_lookup(key, now, "string")
            new = (self._string_value(row[0]) if row else b"") + data
            self._put_string(key, row, new, row[2] if row else None)
        return len(new)

    def strlen(self, name):
        key = self._encode_key(name)
        with self._lock:
            row = self._typed_lookup(key, time.time(), "string")
            return len(self._string_value(row[0])) if row else 0

    def mset(self, mapping):
        now = time.time()
        items = [(self._encode_key(k), self._encode_value(v))
                 for k, v in mapping.items()]
        with self._lock, self._tx_block():
            self._maybe_sweep(now)
            for key, data in items:
                self._put_string(key, self._lookup(key, now), data, None)
        return True

    def mget(self, keys, *args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        names = list(keys) + list(args)
        now = time.time()
        out = []
        with self._lock:
            for name in names:
                row = self._lookup(self._encode_key(name), now)
                if row is None or row[1] != "string":
                    out.append(None)  # MGET treats wrong-type keys as missing
                else:
                    out.append(self._decode(self._string_value(row[0])))
        return out

    # -- counters ---------------------------------------------------------

    def incrby(self, name, amount=1):
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise DataError("amount must be an int")
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._typed_lookup(key, now, "string")
            if row is None:
                current = 0
            else:
                try:
                    current = int(self._string_value(row[0]))
                except ValueError:
                    raise ResponseError(
                        "value is not an integer or out of range"
                    ) from None
            new = current + amount
            self._put_string(key, row, str(new).encode("ascii"),
                             row[2] if row else None)
        return new

    incr = incrby

    def decrby(self, name, amount=1):
        return self.incrby(name, -amount)

    decr = decrby

    # -- hashes -----------------------------------------------------------

    def _hash_lookup(self, key, now):
        return self._typed_lookup(key, now, "hash")

    def hset(self, name, key=None, value=None, mapping=None):
        if key is None and not mapping:
            raise DataError("'hset' with no key/value pairs")
        fields = {}
        if key is not None:
            fields[self._encode_key(key)] = self._encode_value(value)
        if mapping:
            for f, v in mapping.items():
                fields[self._encode_key(f)] = self._encode_value(v)
        name_key = self._encode_key(name)
        now = time.time()
        added = 0
        with self._lock, self._tx_block():
            self._maybe_sweep(now)
            row = self._hash_lookup(name_key, now)
            key_id = row[0] if row else self._create_key(name_key, "hash", None)
            for field, data in fields.items():
                cur = self._conn.execute(
                    "UPDATE babyredis_hashes SET value = ?"
                    " WHERE key_id = ? AND field = ?",
                    (data, key_id, field),
                )
                if cur.rowcount == 0:
                    self._conn.execute(
                        "INSERT INTO babyredis_hashes (key_id, field, value)"
                        " VALUES (?, ?, ?)",
                        (key_id, field, data),
                    )
                    added += 1
        return added

    def hget(self, name, key):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return None
            got = self._conn.execute(
                "SELECT value FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (row[0], field),
            ).fetchone()
        return self._decode(got[0]) if got else None

    def hgetall(self, name):
        name_key = self._encode_key(name)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return {}
            rows = self._conn.execute(
                "SELECT field, value FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return {self._decode_field(f): self._decode(v) for f, v in rows}

    def hdel(self, name, *keys):
        name_key = self._encode_key(name)
        fields = [self._encode_key(k) for k in keys]
        now = time.time()
        with self._lock, self._tx_block():
            row = self._hash_lookup(name_key, now)
            if row is None:
                return 0
            count = 0
            for field in fields:
                cur = self._conn.execute(
                    "DELETE FROM babyredis_hashes"
                    " WHERE key_id = ? AND field = ?",
                    (row[0], field),
                )
                count += cur.rowcount
            (remaining,) = self._conn.execute(
                "SELECT COUNT(*) FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchone()
            if remaining == 0:
                self._conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                )
        return count

    def hexists(self, name, key):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return False
            got = self._conn.execute(
                "SELECT 1 FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (row[0], field),
            ).fetchone()
        return got is not None

    def hkeys(self, name):
        name_key = self._encode_key(name)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return []
            rows = self._conn.execute(
                "SELECT field FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return [self._decode_field(f) for (f,) in rows]

    def hvals(self, name):
        name_key = self._encode_key(name)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return []
            rows = self._conn.execute(
                "SELECT value FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return [self._decode(v) for (v,) in rows]

    def hlen(self, name):
        name_key = self._encode_key(name)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            if row is None:
                return 0
            (count,) = self._conn.execute(
                "SELECT COUNT(*) FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return count

    def hmget(self, name, keys, *args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        fields = [self._encode_key(k) for k in list(keys) + list(args)]
        name_key = self._encode_key(name)
        with self._lock:
            row = self._hash_lookup(name_key, time.time())
            out = []
            for field in fields:
                if row is None:
                    out.append(None)
                    continue
                got = self._conn.execute(
                    "SELECT value FROM babyredis_hashes"
                    " WHERE key_id = ? AND field = ?",
                    (row[0], field),
                ).fetchone()
                out.append(self._decode(got[0]) if got else None)
        return out

    def hsetnx(self, name, key, value):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        data = self._encode_value(value)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._hash_lookup(name_key, now)
            key_id = row[0] if row else self._create_key(name_key, "hash", None)
            cur = self._conn.execute(
                "INSERT INTO babyredis_hashes (key_id, field, value)"
                " VALUES (?, ?, ?) ON CONFLICT(key_id, field) DO NOTHING",
                (key_id, field, data),
            )
        return cur.rowcount == 1

    def hincrby(self, name, key, amount=1):
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise DataError("amount must be an int")
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._hash_lookup(name_key, now)
            key_id = row[0] if row else self._create_key(name_key, "hash", None)
            got = self._conn.execute(
                "SELECT value FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (key_id, field),
            ).fetchone()
            if got is None:
                current = 0
            else:
                try:
                    current = int(bytes(got[0]))
                except ValueError:
                    raise ResponseError(
                        "hash value is not an integer"
                    ) from None
            new = current + amount
            self._conn.execute(
                "INSERT INTO babyredis_hashes (key_id, field, value)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(key_id, field) DO UPDATE SET value=excluded.value",
                (key_id, field, str(new).encode("ascii")),
            )
        return new

    def hstrlen(self, name, key):
        got = self.hget(name, key)
        if got is None:
            return 0
        return len(got if isinstance(got, bytes) else got.encode("utf-8"))

    # -- keys -------------------------------------------------------------

    def delete(self, *names):
        now = time.time()
        count = 0
        with self._lock, self._tx_block():
            for name in names:
                key = self._encode_key(name)
                row = self._lookup(key, now)
                if row is not None:
                    self._conn.execute(
                        "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                    )
                    count += 1
        return count

    def exists(self, *names):
        now = time.time()
        count = 0
        with self._lock:
            for name in names:
                if self._lookup(self._encode_key(name), now) is not None:
                    count += 1
        return count

    def expire(self, name, time_):
        key = self._encode_key(name)
        now = time.time()
        seconds = _to_seconds(time_, "time")
        with self._lock, self._tx_block():
            row = self._lookup(key, now)
            if row is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (now + seconds, row[0]),
            )
        return True

    def pexpire(self, name, time_ms):
        return self.expire(name, _to_seconds(time_ms, "time") / 1000.0)

    def expireat(self, name, when):
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._lookup(key, now)
            if row is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (_to_seconds(when, "when"), row[0]),
            )
        return True

    def persist(self, name):
        key = self._encode_key(name)
        now = time.time()
        with self._lock, self._tx_block():
            row = self._lookup(key, now)
            if row is None or row[2] is None:
                return False
            self._conn.execute(
                "UPDATE babyredis_keys SET expires_at = NULL WHERE id = ?",
                (row[0],),
            )
        return True

    def ttl(self, name):
        pttl = self.pttl(name)
        return pttl if pttl < 0 else max(1, math.ceil(pttl / 1000.0))

    def pttl(self, name):
        key = self._encode_key(name)
        now = time.time()
        with self._lock:
            row = self._lookup(key, now)
        if row is None:
            return -2
        if row[2] is None:
            return -1
        return max(1, math.ceil((row[2] - now) * 1000.0))

    def keys(self, pattern="*"):
        if isinstance(pattern, bytes):
            pattern = pattern.decode("utf-8")
        regex = _glob_to_regex(pattern)
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM babyredis_keys WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchall()
        matched = [k for (k,) in rows if regex.match(k)]
        if self.decode_responses:
            return matched
        return [k.encode("utf-8") for k in matched]

    def type(self, name):
        key = self._encode_key(name)
        with self._lock:
            row = self._lookup(key, time.time())
        result = row[1] if row else "none"
        return result if self.decode_responses else result.encode("ascii")

    # -- server-ish -------------------------------------------------------

    def dbsize(self):
        now = time.time()
        with self._lock:
            (count,) = self._conn.execute(
                "SELECT COUNT(*) FROM babyredis_keys WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchone()
        return count

    def flushdb(self):
        with self._lock:
            self._conn.execute("DELETE FROM babyredis_keys")
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

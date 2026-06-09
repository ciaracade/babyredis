"""SQLite-backed key-value store with a redis-py compatible API.

No server process: data lives in a single SQLite file (WAL mode) so it
persists across restarts and can be shared between processes on the same
machine. Pass ``":memory:"`` for an ephemeral, single-process store.

Schema: a master ``babyredis_keys`` table owns the key name, its type, and
its TTL; one child table per data type holds the payload and cascades on
delete. This keeps expiry and DEL in one place and lets type mismatches
fail with WRONGTYPE like real Redis.

Concurrency: file-backed databases use one connection per thread, so reads
never take a Python lock (WAL gives concurrent readers) and writes
serialize through SQLite's own locking via BEGIN IMMEDIATE. In-memory
databases can't be shared between connections, so they fall back to a
single connection guarded by an RLock.
"""

import math
import random
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


def _score_bound(value):
    """Parse a Redis score bound: number, "(5" exclusive, "-inf"/"+inf"."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value), True
    s = value.decode() if isinstance(value, bytes) else str(value)
    if s.startswith("("):
        return float(s[1:]), False
    return float(s), True


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

CREATE TABLE IF NOT EXISTS babyredis_sets (
  key_id INTEGER NOT NULL
    REFERENCES babyredis_keys(id) ON DELETE CASCADE,
  member BLOB NOT NULL,
  PRIMARY KEY (key_id, member)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS babyredis_zsets (
  key_id INTEGER NOT NULL
    REFERENCES babyredis_keys(id) ON DELETE CASCADE,
  member BLOB NOT NULL,
  score REAL NOT NULL,
  PRIMARY KEY (key_id, member)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS babyredis_zsets_score
  ON babyredis_zsets(key_id, score, member);

CREATE TABLE IF NOT EXISTS babyredis_lists (
  key_id INTEGER NOT NULL
    REFERENCES babyredis_keys(id) ON DELETE CASCADE,
  pos REAL NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (key_id, pos)
) WITHOUT ROWID;
"""

_CHILD_TABLE = {
    "hash": "babyredis_hashes",
    "set": "babyredis_sets",
    "zset": "babyredis_zsets",
    "list": "babyredis_lists",
}


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
        self._path = path
        self._sweep_interval = sweep_interval
        self._last_sweep = 0.0
        self._conns = []
        self._conns_lock = threading.Lock()
        if path == ":memory:":
            self._lock = threading.RLock()
            self._conn = self._connect()
        else:
            self._lock = None
            self._tlocal = threading.local()
            self._get_conn()  # create schema eagerly

    # -- connections ------------------------------------------------------

    def _connect(self):
        conn = sqlite3.connect(
            self._path, isolation_level=None, check_same_thread=False,
            timeout=30.0,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        with self._conns_lock:
            self._conns.append(conn)
        return conn

    def _get_conn(self):
        if self._lock is not None:
            return self._conn
        conn = getattr(self._tlocal, "conn", None)
        if conn is None:
            conn = self._connect()
            self._tlocal.conn = conn
        return conn

    @contextmanager
    def _read(self):
        if self._lock is not None:
            with self._lock:
                yield self._conn
        else:
            yield self._get_conn()

    @contextmanager
    def _tx(self, conn):
        if conn.in_transaction:  # nested (e.g. inside a pipeline)
            yield conn
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    @contextmanager
    def _write(self):
        if self._lock is not None:
            with self._lock, self._tx(self._conn) as conn:
                yield conn
        else:
            with self._tx(self._get_conn()) as conn:
                yield conn

    # -- encoding ---------------------------------------------------------

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

    # -- key bookkeeping --------------------------------------------------

    def _maybe_sweep(self, conn, now):
        if now - self._last_sweep >= self._sweep_interval:
            self._last_sweep = now
            conn.execute(
                "DELETE FROM babyredis_keys WHERE expires_at IS NOT NULL"
                " AND expires_at <= ?", (now,)
            )

    def _lookup(self, conn, key, now):
        """Return (id, type, expires_at) honoring expiry, or None if missing."""
        row = conn.execute(
            "SELECT id, type, expires_at FROM babyredis_keys WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        if row[2] is not None and row[2] <= now:
            conn.execute("DELETE FROM babyredis_keys WHERE id = ?", (row[0],))
            return None
        return row

    def _typed_lookup(self, conn, key, now, expected):
        row = self._lookup(conn, key, now)
        if row is not None and row[1] != expected:
            raise ResponseError(_WRONGTYPE)
        return row

    def _create_key(self, conn, key, type_, expires_at):
        cur = conn.execute(
            "INSERT INTO babyredis_keys (key, type, expires_at)"
            " VALUES (?, ?, ?)",
            (key, type_, expires_at),
        )
        return cur.lastrowid

    def _drop_if_empty(self, conn, key_id, type_):
        table = _CHILD_TABLE[type_]
        (count,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE key_id = ?", (key_id,)
        ).fetchone()
        if count == 0:
            conn.execute("DELETE FROM babyredis_keys WHERE id = ?", (key_id,))
        return count

    # -- strings ----------------------------------------------------------

    def _string_value(self, conn, key_id):
        (value,) = conn.execute(
            "SELECT value FROM babyredis_strings WHERE key_id = ?", (key_id,)
        ).fetchone()
        return bytes(value)

    def _put_string(self, conn, key, row, data, expires_at):
        """Write a string payload, reusing the key row when types match."""
        if row is not None and row[1] == "string":
            conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (expires_at, row[0]),
            )
            conn.execute(
                "UPDATE babyredis_strings SET value = ? WHERE key_id = ?",
                (data, row[0]),
            )
        else:
            if row is not None:
                conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                )
            key_id = self._create_key(conn, key, "string", expires_at)
            conn.execute(
                "INSERT INTO babyredis_strings (key_id, value) VALUES (?, ?)",
                (key_id, data),
            )

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

        with self._write() as conn:
            self._maybe_sweep(conn, now)
            row = self._lookup(conn, key, now)
            if get and row is not None and row[1] != "string":
                raise ResponseError(_WRONGTYPE)
            old = self._string_value(conn, row[0]) \
                if row is not None and row[1] == "string" else None
            should_write = not ((nx and row is not None) or (xx and row is None))
            if should_write:
                if keepttl and row is not None:
                    expires_at = row[2]
                self._put_string(conn, key, row, data, expires_at)
        if get:
            return self._decode(old)
        return True if should_write else None

    def get(self, name):
        key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, key, time.time(), "string")
            if row is None:
                return None
            return self._decode(self._string_value(conn, row[0]))

    def getdel(self, name):
        key = self._encode_key(name)
        with self._write() as conn:
            row = self._typed_lookup(conn, key, time.time(), "string")
            if row is None:
                return None
            old = self._string_value(conn, row[0])
            conn.execute("DELETE FROM babyredis_keys WHERE id = ?", (row[0],))
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
        with self._write() as conn:
            row = self._typed_lookup(conn, key, now, "string")
            new = (self._string_value(conn, row[0]) if row else b"") + data
            self._put_string(conn, key, row, new, row[2] if row else None)
        return len(new)

    def strlen(self, name):
        key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, key, time.time(), "string")
            return len(self._string_value(conn, row[0])) if row else 0

    def mset(self, mapping):
        now = time.time()
        items = [(self._encode_key(k), self._encode_value(v))
                 for k, v in mapping.items()]
        with self._write() as conn:
            self._maybe_sweep(conn, now)
            for key, data in items:
                self._put_string(conn, key, self._lookup(conn, key, now),
                                 data, None)
        return True

    def mget(self, keys, *args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        names = list(keys) + list(args)
        now = time.time()
        out = []
        with self._read() as conn:
            for name in names:
                row = self._lookup(conn, self._encode_key(name), now)
                if row is None or row[1] != "string":
                    out.append(None)  # MGET treats wrong-type keys as missing
                else:
                    out.append(self._decode(self._string_value(conn, row[0])))
        return out

    # -- counters ---------------------------------------------------------

    def incrby(self, name, amount=1):
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise DataError("amount must be an int")
        key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, key, now, "string")
            if row is None:
                current = 0
            else:
                try:
                    current = int(self._string_value(conn, row[0]))
                except ValueError:
                    raise ResponseError(
                        "value is not an integer or out of range"
                    ) from None
            new = current + amount
            self._put_string(conn, key, row, str(new).encode("ascii"),
                             row[2] if row else None)
        return new

    incr = incrby

    def decrby(self, name, amount=1):
        return self.incrby(name, -amount)

    decr = decrby

    @staticmethod
    def _format_float(value):
        # Redis strips a trailing ".0" from float results
        s = repr(value)
        return s[:-2] if s.endswith(".0") else s

    def incrbyfloat(self, name, amount=1.0):
        key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, key, now, "string")
            if row is None:
                current = 0.0
            else:
                try:
                    current = float(self._string_value(conn, row[0]))
                except ValueError:
                    raise ResponseError(
                        "value is not a valid float"
                    ) from None
            new = current + float(amount)
            self._put_string(conn, key, row,
                             self._format_float(new).encode("ascii"),
                             row[2] if row else None)
        return new

    # -- hashes -----------------------------------------------------------

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
        with self._write() as conn:
            self._maybe_sweep(conn, now)
            row = self._typed_lookup(conn, name_key, now, "hash")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "hash", None)
            for field, data in fields.items():
                cur = conn.execute(
                    "UPDATE babyredis_hashes SET value = ?"
                    " WHERE key_id = ? AND field = ?",
                    (data, key_id, field),
                )
                if cur.rowcount == 0:
                    conn.execute(
                        "INSERT INTO babyredis_hashes (key_id, field, value)"
                        " VALUES (?, ?, ?)",
                        (key_id, field, data),
                    )
                    added += 1
        return added

    def hget(self, name, key):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return None
            got = conn.execute(
                "SELECT value FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (row[0], field),
            ).fetchone()
        return self._decode(got[0]) if got else None

    def hgetall(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return {}
            rows = conn.execute(
                "SELECT field, value FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return {self._decode_field(f): self._decode(v) for f, v in rows}

    def hdel(self, name, *keys):
        if not keys:
            raise DataError("'hdel' with no fields")
        name_key = self._encode_key(name)
        fields = [self._encode_key(k) for k in keys]
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "hash")
            if row is None:
                return 0
            count = 0
            for field in fields:
                cur = conn.execute(
                    "DELETE FROM babyredis_hashes"
                    " WHERE key_id = ? AND field = ?",
                    (row[0], field),
                )
                count += cur.rowcount
            self._drop_if_empty(conn, row[0], "hash")
        return count

    def hexists(self, name, key):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return False
            got = conn.execute(
                "SELECT 1 FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (row[0], field),
            ).fetchone()
        return got is not None

    def hkeys(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return []
            rows = conn.execute(
                "SELECT field FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return [self._decode_field(f) for (f,) in rows]

    def hvals(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return []
            rows = conn.execute(
                "SELECT value FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchall()
        return [self._decode(v) for (v,) in rows]

    def hlen(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return 0
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_hashes WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return count

    def hmget(self, name, keys, *args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        fields = [self._encode_key(k) for k in list(keys) + list(args)]
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            out = []
            for field in fields:
                if row is None:
                    out.append(None)
                    continue
                got = conn.execute(
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
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "hash")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "hash", None)
            cur = conn.execute(
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
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "hash")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "hash", None)
            got = conn.execute(
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
            conn.execute(
                "INSERT INTO babyredis_hashes (key_id, field, value)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(key_id, field) DO UPDATE SET value=excluded.value",
                (key_id, field, str(new).encode("ascii")),
            )
        return new

    def hincrbyfloat(self, name, key, amount=1.0):
        name_key = self._encode_key(name)
        field = self._encode_key(key)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "hash")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "hash", None)
            got = conn.execute(
                "SELECT value FROM babyredis_hashes"
                " WHERE key_id = ? AND field = ?",
                (key_id, field),
            ).fetchone()
            if got is None:
                current = 0.0
            else:
                try:
                    current = float(bytes(got[0]))
                except ValueError:
                    raise ResponseError(
                        "hash value is not a float"
                    ) from None
            new = current + float(amount)
            conn.execute(
                "INSERT INTO babyredis_hashes (key_id, field, value)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(key_id, field) DO UPDATE SET value=excluded.value",
                (key_id, field, self._format_float(new).encode("ascii")),
            )
        return new

    def hstrlen(self, name, key):
        got = self.hget(name, key)
        if got is None:
            return 0
        return len(got if isinstance(got, bytes) else got.encode("utf-8"))

    def hscan(self, name, cursor=0, match=None, count=10):
        name_key = self._encode_key(name)
        regex = None
        if match is not None:
            if isinstance(match, bytes):
                match = match.decode("utf-8")
            regex = _glob_to_regex(match)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "hash")
            if row is None:
                return 0, {}
            rows = conn.execute(
                "SELECT field, value FROM babyredis_hashes WHERE key_id = ?"
                " ORDER BY field LIMIT ? OFFSET ?",
                (row[0], count, cursor),
            ).fetchall()
        next_cursor = 0 if len(rows) < count else cursor + len(rows)
        out = {self._decode_field(f): self._decode(v) for f, v in rows
               if regex is None or regex.match(f)}
        return next_cursor, out

    def hscan_iter(self, name, match=None, count=10):
        cursor = 0
        while True:
            cursor, page = self.hscan(name, cursor, match=match, count=count)
            yield from page.items()
            if cursor == 0:
                return

    # -- sets ---------------------------------------------------------------

    def sadd(self, name, *values):
        if not values:
            raise DataError("'sadd' with no members")
        name_key = self._encode_key(name)
        members = [self._encode_value(v) for v in values]
        now = time.time()
        added = 0
        with self._write() as conn:
            self._maybe_sweep(conn, now)
            row = self._typed_lookup(conn, name_key, now, "set")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "set", None)
            for member in members:
                cur = conn.execute(
                    "INSERT INTO babyredis_sets (key_id, member)"
                    " VALUES (?, ?) ON CONFLICT(key_id, member) DO NOTHING",
                    (key_id, member),
                )
                added += cur.rowcount
        return added

    def srem(self, name, *values):
        name_key = self._encode_key(name)
        members = [self._encode_value(v) for v in values]
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "set")
            if row is None:
                return 0
            count = 0
            for member in members:
                cur = conn.execute(
                    "DELETE FROM babyredis_sets"
                    " WHERE key_id = ? AND member = ?",
                    (row[0], member),
                )
                count += cur.rowcount
            self._drop_if_empty(conn, row[0], "set")
        return count

    def _set_members(self, conn, name, now):
        row = self._typed_lookup(conn, self._encode_key(name), now, "set")
        if row is None:
            return set()
        rows = conn.execute(
            "SELECT member FROM babyredis_sets WHERE key_id = ?", (row[0],)
        ).fetchall()
        return {bytes(m) for (m,) in rows}

    def smembers(self, name):
        with self._read() as conn:
            members = self._set_members(conn, name, time.time())
        return {self._decode(m) for m in members}

    def sismember(self, name, value):
        name_key = self._encode_key(name)
        member = self._encode_value(value)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "set")
            if row is None:
                return False
            got = conn.execute(
                "SELECT 1 FROM babyredis_sets WHERE key_id = ? AND member = ?",
                (row[0], member),
            ).fetchone()
        return got is not None

    def scard(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "set")
            if row is None:
                return 0
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_sets WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return count

    def spop(self, name, count=None):
        name_key = self._encode_key(name)
        now = time.time()
        n = 1 if count is None else count
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "set")
            if row is None:
                return None if count is None else []
            rows = conn.execute(
                "SELECT member FROM babyredis_sets WHERE key_id = ?"
                " ORDER BY RANDOM() LIMIT ?",
                (row[0], n),
            ).fetchall()
            popped = [bytes(m) for (m,) in rows]
            for member in popped:
                conn.execute(
                    "DELETE FROM babyredis_sets"
                    " WHERE key_id = ? AND member = ?",
                    (row[0], member),
                )
            self._drop_if_empty(conn, row[0], "set")
        if count is None:
            return self._decode(popped[0]) if popped else None
        return [self._decode(m) for m in popped]

    def srandmember(self, name, number=None):
        with self._read() as conn:
            members = sorted(self._set_members(conn, name, time.time()))
        if number is None:
            return self._decode(random.choice(members)) if members else None
        if not members:
            return []
        if number < 0:  # negative allows repeats, like Redis
            return [self._decode(random.choice(members))
                    for _ in range(-number)]
        picked = random.sample(members, min(number, len(members)))
        return [self._decode(m) for m in picked]

    def smove(self, src, dst, value):
        member = self._encode_value(value)
        now = time.time()
        with self._write() as conn:
            src_row = self._typed_lookup(conn, self._encode_key(src), now, "set")
            if src_row is None:
                # like Redis: missing source short-circuits before the
                # destination type check
                return False
            dst_row = self._typed_lookup(conn, self._encode_key(dst), now, "set")
            cur = conn.execute(
                "DELETE FROM babyredis_sets WHERE key_id = ? AND member = ?",
                (src_row[0], member),
            )
            if cur.rowcount == 0:
                return False
            dst_id = dst_row[0] if dst_row else self._create_key(
                conn, self._encode_key(dst), "set", None)
            conn.execute(
                "INSERT INTO babyredis_sets (key_id, member) VALUES (?, ?)"
                " ON CONFLICT(key_id, member) DO NOTHING",
                (dst_id, member),
            )
            self._drop_if_empty(conn, src_row[0], "set")
        return True

    def _set_op(self, op, keys, args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        names = list(keys) + list(args)
        now = time.time()
        with self._read() as conn:
            sets = [self._set_members(conn, name, now) for name in names]
        if not sets:
            return set()
        result = sets[0]
        for other in sets[1:]:
            result = op(result, other)
        return {self._decode(m) for m in result}

    def sinter(self, keys, *args):
        return self._set_op(set.intersection, keys, args)

    def sunion(self, keys, *args):
        return self._set_op(set.union, keys, args)

    def sdiff(self, keys, *args):
        return self._set_op(set.difference, keys, args)

    def smismember(self, name, values):
        return [self.sismember(name, v) for v in values]

    def _set_store(self, op, dest, keys, args):
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        names = list(keys) + list(args)
        dest_key = self._encode_key(dest)
        now = time.time()
        with self._write() as conn:
            sets = [self._set_members(conn, name, now) for name in names]
            result = sets[0] if sets else set()
            for other in sets[1:]:
                result = op(result, other)
            dest_row = self._lookup(conn, dest_key, now)
            if dest_row is not None:
                conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (dest_row[0],)
                )
            if result:  # like Redis, an empty result deletes dest
                key_id = self._create_key(conn, dest_key, "set", None)
                for member in result:
                    conn.execute(
                        "INSERT INTO babyredis_sets (key_id, member)"
                        " VALUES (?, ?)",
                        (key_id, member),
                    )
        return len(result)

    def sinterstore(self, dest, keys, *args):
        return self._set_store(set.intersection, dest, keys, args)

    def sunionstore(self, dest, keys, *args):
        return self._set_store(set.union, dest, keys, args)

    def sdiffstore(self, dest, keys, *args):
        return self._set_store(set.difference, dest, keys, args)

    def sscan(self, name, cursor=0, match=None, count=10):
        name_key = self._encode_key(name)
        regex = None
        if match is not None:
            if isinstance(match, bytes):
                match = match.decode("utf-8")
            regex = _glob_to_regex(match)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "set")
            if row is None:
                return 0, []
            rows = conn.execute(
                "SELECT member FROM babyredis_sets WHERE key_id = ?"
                " ORDER BY member LIMIT ? OFFSET ?",
                (row[0], count, cursor),
            ).fetchall()
        next_cursor = 0 if len(rows) < count else cursor + len(rows)
        out = []
        for (m,) in rows:
            m = bytes(m)
            if regex is None or regex.match(m.decode("utf-8", "replace")):
                out.append(self._decode(m))
        return next_cursor, out

    def sscan_iter(self, name, match=None, count=10):
        cursor = 0
        while True:
            cursor, page = self.sscan(name, cursor, match=match, count=count)
            yield from page
            if cursor == 0:
                return

    # -- sorted sets --------------------------------------------------------

    def zadd(self, name, mapping, nx=False, xx=False, gt=False, lt=False,
             ch=False, incr=False):
        if not mapping:
            raise DataError("'zadd' with no member/score pairs")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError("ZADD with 'incr' takes exactly one member")
        items = [(self._encode_value(m), float(s)) for m, s in mapping.items()]
        name_key = self._encode_key(name)
        now = time.time()
        added = changed = 0
        incr_result = None
        with self._write() as conn:
            self._maybe_sweep(conn, now)
            row = self._typed_lookup(conn, name_key, now, "zset")
            key_id = row[0] if row else None
            for member, score in items:
                got = None
                if key_id is not None:
                    got = conn.execute(
                        "SELECT score FROM babyredis_zsets"
                        " WHERE key_id = ? AND member = ?",
                        (key_id, member),
                    ).fetchone()
                if got is None:
                    if xx:
                        continue
                    if key_id is None:
                        key_id = self._create_key(conn, name_key, "zset", None)
                    conn.execute(
                        "INSERT INTO babyredis_zsets (key_id, member, score)"
                        " VALUES (?, ?, ?)",
                        (key_id, member, score),
                    )
                    added += 1
                    changed += 1
                    incr_result = score
                else:
                    if nx:
                        continue
                    current = got[0]
                    new = current + score if incr else score
                    if gt and not new > current:
                        continue
                    if lt and not new < current:
                        continue
                    if new != current:
                        conn.execute(
                            "UPDATE babyredis_zsets SET score = ?"
                            " WHERE key_id = ? AND member = ?",
                            (new, key_id, member),
                        )
                        changed += 1
                    incr_result = new
        if incr:
            return incr_result
        return changed if ch else added

    def zincrby(self, name, amount, value):
        return self.zadd(name, {value: float(amount)}, incr=True)

    def zscore(self, name, value):
        name_key = self._encode_key(name)
        member = self._encode_value(value)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return None
            got = conn.execute(
                "SELECT score FROM babyredis_zsets"
                " WHERE key_id = ? AND member = ?",
                (row[0], member),
            ).fetchone()
        return float(got[0]) if got else None

    def zmscore(self, name, members):
        return [self.zscore(name, m) for m in members]

    def zrem(self, name, *values):
        name_key = self._encode_key(name)
        members = [self._encode_value(v) for v in values]
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "zset")
            if row is None:
                return 0
            count = 0
            for member in members:
                cur = conn.execute(
                    "DELETE FROM babyredis_zsets"
                    " WHERE key_id = ? AND member = ?",
                    (row[0], member),
                )
                count += cur.rowcount
            self._drop_if_empty(conn, row[0], "zset")
        return count

    def zcard(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return 0
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_zsets WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return count

    @staticmethod
    def _score_where(min_, max_):
        lo, lo_inc = _score_bound(min_)
        hi, hi_inc = _score_bound(max_)
        clauses, params = [], []
        if lo != float("-inf"):
            clauses.append("score >= ?" if lo_inc else "score > ?")
            params.append(lo)
        if hi != float("inf"):
            clauses.append("score <= ?" if hi_inc else "score < ?")
            params.append(hi)
        where = (" AND " + " AND ".join(clauses)) if clauses else ""
        return where, params

    def zcount(self, name, min, max):
        name_key = self._encode_key(name)
        where, params = self._score_where(min, max)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return 0
            (count,) = conn.execute(
                f"SELECT COUNT(*) FROM babyredis_zsets WHERE key_id = ?{where}",
                [row[0]] + params,
            ).fetchone()
        return count

    def _zrange_rows(self, conn, key_id, start, end, desc):
        (total,) = conn.execute(
            "SELECT COUNT(*) FROM babyredis_zsets WHERE key_id = ?", (key_id,)
        ).fetchone()
        if start < 0:
            start = max(total + start, 0)
        if end < 0:
            end = total + end
        if start > end or start >= total:
            return []
        end = min(end, total - 1)
        order = "DESC" if desc else "ASC"
        return conn.execute(
            f"SELECT member, score FROM babyredis_zsets WHERE key_id = ?"
            f" ORDER BY score {order}, member {order} LIMIT ? OFFSET ?",
            (key_id, end - start + 1, start),
        ).fetchall()

    def _zformat(self, rows, withscores):
        if withscores:
            return [(self._decode(bytes(m)), float(s)) for m, s in rows]
        return [self._decode(bytes(m)) for m, _ in rows]

    def zrange(self, name, start, end, desc=False, withscores=False):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return []
            rows = self._zrange_rows(conn, row[0], start, end, desc)
        return self._zformat(rows, withscores)

    def zrevrange(self, name, start, end, withscores=False):
        return self.zrange(name, start, end, desc=True, withscores=withscores)

    def _zrangebyscore_rows(self, name, min_, max_, start, num, desc,
                            withscores):
        name_key = self._encode_key(name)
        where, params = self._score_where(min_, max_)
        order = "DESC" if desc else "ASC"
        limit = ""
        limit_params = []
        if start is not None or num is not None:
            limit = " LIMIT ? OFFSET ?"
            limit_params = [num if num is not None else -1,
                            start if start is not None else 0]
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return []
            rows = conn.execute(
                f"SELECT member, score FROM babyredis_zsets WHERE key_id = ?"
                f"{where} ORDER BY score {order}, member {order}{limit}",
                [row[0]] + params + limit_params,
            ).fetchall()
        return self._zformat(rows, withscores)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False):
        return self._zrangebyscore_rows(name, min, max, start, num,
                                        desc=False, withscores=withscores)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False):
        return self._zrangebyscore_rows(name, min, max, start, num,
                                        desc=True, withscores=withscores)

    def _zrank(self, name, value, desc):
        name_key = self._encode_key(name)
        member = self._encode_value(value)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return None
            got = conn.execute(
                "SELECT score FROM babyredis_zsets"
                " WHERE key_id = ? AND member = ?",
                (row[0], member),
            ).fetchone()
            if got is None:
                return None
            score = got[0]
            if desc:
                cond = "(score > ?) OR (score = ? AND member > ?)"
            else:
                cond = "(score < ?) OR (score = ? AND member < ?)"
            (rank,) = conn.execute(
                f"SELECT COUNT(*) FROM babyredis_zsets WHERE key_id = ?"
                f" AND ({cond})",
                (row[0], score, score, member),
            ).fetchone()
        return rank

    def zrank(self, name, value):
        return self._zrank(name, value, desc=False)

    def zrevrank(self, name, value):
        return self._zrank(name, value, desc=True)

    def _zpop(self, name, count, desc):
        name_key = self._encode_key(name)
        now = time.time()
        n = 1 if count is None else count
        order = "DESC" if desc else "ASC"
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "zset")
            if row is None:
                return []
            rows = conn.execute(
                f"SELECT member, score FROM babyredis_zsets WHERE key_id = ?"
                f" ORDER BY score {order}, member {order} LIMIT ?",
                (row[0], n),
            ).fetchall()
            for member, _ in rows:
                conn.execute(
                    "DELETE FROM babyredis_zsets"
                    " WHERE key_id = ? AND member = ?",
                    (row[0], bytes(member)),
                )
            self._drop_if_empty(conn, row[0], "zset")
        return [(self._decode(bytes(m)), float(s)) for m, s in rows]

    def zpopmin(self, name, count=None):
        return self._zpop(name, count, desc=False)

    def zpopmax(self, name, count=None):
        return self._zpop(name, count, desc=True)

    def zremrangebyrank(self, name, min, max):
        name_key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "zset")
            if row is None:
                return 0
            rows = self._zrange_rows(conn, row[0], min, max, desc=False)
            for member, _ in rows:
                conn.execute(
                    "DELETE FROM babyredis_zsets"
                    " WHERE key_id = ? AND member = ?",
                    (row[0], bytes(member)),
                )
            self._drop_if_empty(conn, row[0], "zset")
        return len(rows)

    def zremrangebyscore(self, name, min, max):
        name_key = self._encode_key(name)
        where, params = self._score_where(min, max)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "zset")
            if row is None:
                return 0
            cur = conn.execute(
                f"DELETE FROM babyredis_zsets WHERE key_id = ?{where}",
                [row[0]] + params,
            )
            removed = cur.rowcount
            self._drop_if_empty(conn, row[0], "zset")
        return removed

    def _zset_or_set_scores(self, conn, name, now):
        """Member->score map; plain sets count as score 1.0 (like Redis)."""
        row = self._lookup(conn, self._encode_key(name), now)
        if row is None:
            return None
        if row[1] == "zset":
            rows = conn.execute(
                "SELECT member, score FROM babyredis_zsets WHERE key_id = ?",
                (row[0],),
            ).fetchall()
            return {bytes(m): float(s) for m, s in rows}
        if row[1] == "set":
            rows = conn.execute(
                "SELECT member FROM babyredis_sets WHERE key_id = ?",
                (row[0],),
            ).fetchall()
            return {bytes(m): 1.0 for (m,) in rows}
        raise ResponseError(_WRONGTYPE)

    def _zstore(self, dest, keys, aggregate, inter):
        if isinstance(keys, dict):
            items = [(k, float(w)) for k, w in keys.items()]
        else:
            items = [(k, 1.0) for k in keys]
        agg = (aggregate or "SUM").upper()
        if agg not in ("SUM", "MIN", "MAX"):
            raise DataError("aggregate must be SUM, MIN, or MAX")
        combine = {"SUM": lambda a, b: a + b, "MIN": min, "MAX": max}[agg]
        dest_key = self._encode_key(dest)
        now = time.time()
        with self._write() as conn:
            maps = []
            for name, weight in items:
                scores = self._zset_or_set_scores(conn, name, now)
                if scores is None:
                    maps.append({})
                else:
                    maps.append({m: s * weight for m, s in scores.items()})
            result = {}
            if maps:
                if inter:
                    members = set(maps[0])
                    for other in maps[1:]:
                        members &= set(other)
                else:
                    members = set()
                    for m in maps:
                        members |= set(m)
                for member in members:
                    scores = [m[member] for m in maps if member in m]
                    acc = scores[0]
                    for s in scores[1:]:
                        acc = combine(acc, s)
                    result[member] = acc
            dest_row = self._lookup(conn, dest_key, now)
            if dest_row is not None:
                conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (dest_row[0],)
                )
            if result:
                key_id = self._create_key(conn, dest_key, "zset", None)
                for member, score in result.items():
                    conn.execute(
                        "INSERT INTO babyredis_zsets (key_id, member, score)"
                        " VALUES (?, ?, ?)",
                        (key_id, member, score),
                    )
        return len(result)

    def zunionstore(self, dest, keys, aggregate=None):
        return self._zstore(dest, keys, aggregate, inter=False)

    def zinterstore(self, dest, keys, aggregate=None):
        return self._zstore(dest, keys, aggregate, inter=True)

    def zscan(self, name, cursor=0, match=None, count=10):
        name_key = self._encode_key(name)
        regex = None
        if match is not None:
            if isinstance(match, bytes):
                match = match.decode("utf-8")
            regex = _glob_to_regex(match)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "zset")
            if row is None:
                return 0, []
            rows = conn.execute(
                "SELECT member, score FROM babyredis_zsets WHERE key_id = ?"
                " ORDER BY member LIMIT ? OFFSET ?",
                (row[0], count, cursor),
            ).fetchall()
        next_cursor = 0 if len(rows) < count else cursor + len(rows)
        out = []
        for m, s in rows:
            m = bytes(m)
            if regex is None or regex.match(m.decode("utf-8", "replace")):
                out.append((self._decode(m), float(s)))
        return next_cursor, out

    def zscan_iter(self, name, match=None, count=10):
        cursor = 0
        while True:
            cursor, page = self.zscan(name, cursor, match=match, count=count)
            yield from page
            if cursor == 0:
                return

    # -- lists ----------------------------------------------------------------

    def _list_bounds(self, conn, key_id):
        return conn.execute(
            "SELECT MIN(pos), MAX(pos) FROM babyredis_lists WHERE key_id = ?",
            (key_id,),
        ).fetchone()

    def _list_renumber(self, conn, key_id):
        rows = conn.execute(
            "SELECT pos, value FROM babyredis_lists WHERE key_id = ?"
            " ORDER BY pos", (key_id,)
        ).fetchall()
        conn.execute(
            "DELETE FROM babyredis_lists WHERE key_id = ?", (key_id,)
        )
        for i, (_, value) in enumerate(rows):
            conn.execute(
                "INSERT INTO babyredis_lists (key_id, pos, value)"
                " VALUES (?, ?, ?)",
                (key_id, float(i), bytes(value)),
            )

    def _push(self, name, values, left):
        if not values:
            raise DataError("push with no values")
        name_key = self._encode_key(name)
        items = [self._encode_value(v) for v in values]
        now = time.time()
        with self._write() as conn:
            self._maybe_sweep(conn, now)
            row = self._typed_lookup(conn, name_key, now, "list")
            key_id = row[0] if row else self._create_key(
                conn, name_key, "list", None)
            lo, hi = self._list_bounds(conn, key_id)
            for value in items:
                if left:
                    pos = (lo if lo is not None else 0.0) - 1.0
                    lo = pos
                    hi = hi if hi is not None else pos
                else:
                    pos = (hi if hi is not None else 0.0) + 1.0
                    hi = pos
                    lo = lo if lo is not None else pos
                conn.execute(
                    "INSERT INTO babyredis_lists (key_id, pos, value)"
                    " VALUES (?, ?, ?)",
                    (key_id, pos, value),
                )
            (length,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_lists WHERE key_id = ?",
                (key_id,),
            ).fetchone()
        return length

    def lpush(self, name, *values):
        return self._push(name, values, left=True)

    def rpush(self, name, *values):
        return self._push(name, values, left=False)

    def _pop(self, name, count, left):
        name_key = self._encode_key(name)
        now = time.time()
        n = 1 if count is None else count
        order = "ASC" if left else "DESC"
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "list")
            if row is None:
                return None if count is None else None
            rows = conn.execute(
                f"SELECT pos, value FROM babyredis_lists WHERE key_id = ?"
                f" ORDER BY pos {order} LIMIT ?",
                (row[0], n),
            ).fetchall()
            for pos, _ in rows:
                conn.execute(
                    "DELETE FROM babyredis_lists"
                    " WHERE key_id = ? AND pos = ?",
                    (row[0], pos),
                )
            self._drop_if_empty(conn, row[0], "list")
        values = [self._decode(bytes(v)) for _, v in rows]
        if count is None:
            return values[0] if values else None
        return values  # count=0 on an existing key yields [], like Redis

    def lpop(self, name, count=None):
        return self._pop(name, count, left=True)

    def rpop(self, name, count=None):
        return self._pop(name, count, left=False)

    def llen(self, name):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "list")
            if row is None:
                return 0
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_lists WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return count

    def _range_window(self, conn, key_id, start, end):
        (total,) = conn.execute(
            "SELECT COUNT(*) FROM babyredis_lists WHERE key_id = ?", (key_id,)
        ).fetchone()
        if start < 0:
            start = max(total + start, 0)
        if end < 0:
            end = total + end
        if start > end or start >= total:
            return None, None, total
        return start, min(end, total - 1), total

    def lrange(self, name, start, end):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "list")
            if row is None:
                return []
            lo, hi, _ = self._range_window(conn, row[0], start, end)
            if lo is None:
                return []
            rows = conn.execute(
                "SELECT value FROM babyredis_lists WHERE key_id = ?"
                " ORDER BY pos LIMIT ? OFFSET ?",
                (row[0], hi - lo + 1, lo),
            ).fetchall()
        return [self._decode(bytes(v)) for (v,) in rows]

    def lindex(self, name, index):
        name_key = self._encode_key(name)
        with self._read() as conn:
            row = self._typed_lookup(conn, name_key, time.time(), "list")
            if row is None:
                return None
            lo, hi, _ = self._range_window(conn, row[0], index, index)
            if lo is None:
                return None
            got = conn.execute(
                "SELECT value FROM babyredis_lists WHERE key_id = ?"
                " ORDER BY pos LIMIT 1 OFFSET ?",
                (row[0], lo),
            ).fetchone()
        return self._decode(bytes(got[0])) if got else None

    def lset(self, name, index, value):
        name_key = self._encode_key(name)
        data = self._encode_value(value)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "list")
            if row is None:
                raise ResponseError("no such key")
            lo, _, total = self._range_window(conn, row[0], index, index)
            if lo is None:
                raise ResponseError("index out of range")
            got = conn.execute(
                "SELECT pos FROM babyredis_lists WHERE key_id = ?"
                " ORDER BY pos LIMIT 1 OFFSET ?",
                (row[0], lo),
            ).fetchone()
            conn.execute(
                "UPDATE babyredis_lists SET value = ?"
                " WHERE key_id = ? AND pos = ?",
                (data, row[0], got[0]),
            )
        return True

    def lrem(self, name, count, value):
        name_key = self._encode_key(name)
        data = self._encode_value(value)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "list")
            if row is None:
                return 0
            order = "DESC" if count < 0 else "ASC"
            limit = abs(count) if count != 0 else -1
            rows = conn.execute(
                f"SELECT pos FROM babyredis_lists"
                f" WHERE key_id = ? AND value = ? ORDER BY pos {order}"
                f" LIMIT ?",
                (row[0], data, limit),
            ).fetchall()
            for (pos,) in rows:
                conn.execute(
                    "DELETE FROM babyredis_lists"
                    " WHERE key_id = ? AND pos = ?",
                    (row[0], pos),
                )
            self._drop_if_empty(conn, row[0], "list")
        return len(rows)

    def ltrim(self, name, start, end):
        name_key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "list")
            if row is None:
                return True
            lo, hi, total = self._range_window(conn, row[0], start, end)
            if lo is None:  # nothing kept: empty list means delete the key
                conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                )
                return True
            keep = conn.execute(
                "SELECT pos FROM babyredis_lists WHERE key_id = ?"
                " ORDER BY pos LIMIT ? OFFSET ?",
                (row[0], hi - lo + 1, lo),
            ).fetchall()
            lo_pos, hi_pos = keep[0][0], keep[-1][0]
            conn.execute(
                "DELETE FROM babyredis_lists"
                " WHERE key_id = ? AND (pos < ? OR pos > ?)",
                (row[0], lo_pos, hi_pos),
            )
        return True

    def linsert(self, name, where, refvalue, value):
        where = where.lower()
        if where not in ("before", "after"):
            raise DataError("where must be 'before' or 'after'")
        name_key = self._encode_key(name)
        ref = self._encode_value(refvalue)
        data = self._encode_value(value)
        now = time.time()
        with self._write() as conn:
            row = self._typed_lookup(conn, name_key, now, "list")
            if row is None:
                return 0
            for _ in range(2):  # second pass after renumbering if needed
                got = conn.execute(
                    "SELECT pos FROM babyredis_lists"
                    " WHERE key_id = ? AND value = ? ORDER BY pos LIMIT 1",
                    (row[0], ref),
                ).fetchone()
                if got is None:
                    return -1
                ref_pos = got[0]
                if where == "before":
                    neighbor = conn.execute(
                        "SELECT MAX(pos) FROM babyredis_lists"
                        " WHERE key_id = ? AND pos < ?",
                        (row[0], ref_pos),
                    ).fetchone()[0]
                    other = neighbor if neighbor is not None else ref_pos - 2.0
                else:
                    neighbor = conn.execute(
                        "SELECT MIN(pos) FROM babyredis_lists"
                        " WHERE key_id = ? AND pos > ?",
                        (row[0], ref_pos),
                    ).fetchone()[0]
                    other = neighbor if neighbor is not None else ref_pos + 2.0
                new_pos = (ref_pos + other) / 2.0
                if new_pos != ref_pos and new_pos != other:
                    conn.execute(
                        "INSERT INTO babyredis_lists (key_id, pos, value)"
                        " VALUES (?, ?, ?)",
                        (row[0], new_pos, data),
                    )
                    break
                self._list_renumber(conn, row[0])  # float precision exhausted
            (length,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_lists WHERE key_id = ?",
                (row[0],),
            ).fetchone()
        return length

    def lmove(self, first_list, second_list, src="LEFT", dest="RIGHT"):
        src, dest = src.upper(), dest.upper()
        if src not in ("LEFT", "RIGHT") or dest not in ("LEFT", "RIGHT"):
            raise DataError("src and dest must be 'LEFT' or 'RIGHT'")
        src_key = self._encode_key(first_list)
        dst_key = self._encode_key(second_list)
        now = time.time()
        with self._write() as conn:
            src_row = self._typed_lookup(conn, src_key, now, "list")
            if src_row is None:
                # Redis returns nil without type-checking the destination
                return None
            dst_row = src_row if src_key == dst_key \
                else self._typed_lookup(conn, dst_key, now, "list")
            order = "ASC" if src == "LEFT" else "DESC"
            pos, value = conn.execute(
                f"SELECT pos, value FROM babyredis_lists WHERE key_id = ?"
                f" ORDER BY pos {order} LIMIT 1",
                (src_row[0],),
            ).fetchone()
            conn.execute(
                "DELETE FROM babyredis_lists WHERE key_id = ? AND pos = ?",
                (src_row[0], pos),
            )
            dst_id = dst_row[0] if dst_row is not None else self._create_key(
                conn, dst_key, "list", None)
            lo, hi = self._list_bounds(conn, dst_id)
            if dest == "LEFT":
                new_pos = (lo if lo is not None else 0.0) - 1.0
            else:
                new_pos = (hi if hi is not None else 0.0) + 1.0
            conn.execute(
                "INSERT INTO babyredis_lists (key_id, pos, value)"
                " VALUES (?, ?, ?)",
                (dst_id, new_pos, bytes(value)),
            )
            if src_row[0] != dst_id:
                self._drop_if_empty(conn, src_row[0], "list")
        return self._decode(bytes(value))

    def rpoplpush(self, src, dst):
        return self.lmove(src, dst, "RIGHT", "LEFT")

    # -- keys -------------------------------------------------------------

    def delete(self, *names):
        now = time.time()
        count = 0
        with self._write() as conn:
            for name in names:
                key = self._encode_key(name)
                row = self._lookup(conn, key, now)
                if row is not None:
                    conn.execute(
                        "DELETE FROM babyredis_keys WHERE id = ?", (row[0],)
                    )
                    count += 1
        return count

    def exists(self, *names):
        now = time.time()
        count = 0
        with self._read() as conn:
            for name in names:
                if self._lookup(conn, self._encode_key(name), now) is not None:
                    count += 1
        return count

    def expire(self, name, time_):
        key = self._encode_key(name)
        now = time.time()
        seconds = _to_seconds(time_, "time")
        with self._write() as conn:
            row = self._lookup(conn, key, now)
            if row is None:
                return False
            conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (now + seconds, row[0]),
            )
        return True

    def pexpire(self, name, time_ms):
        return self.expire(name, _to_seconds(time_ms, "time") / 1000.0)

    def expireat(self, name, when):
        key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._lookup(conn, key, now)
            if row is None:
                return False
            conn.execute(
                "UPDATE babyredis_keys SET expires_at = ? WHERE id = ?",
                (_to_seconds(when, "when"), row[0]),
            )
        return True

    def persist(self, name):
        key = self._encode_key(name)
        now = time.time()
        with self._write() as conn:
            row = self._lookup(conn, key, now)
            if row is None or row[2] is None:
                return False
            conn.execute(
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
        with self._read() as conn:
            row = self._lookup(conn, key, now)
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
        with self._read() as conn:
            rows = conn.execute(
                "SELECT key FROM babyredis_keys WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchall()
        matched = [k for (k,) in rows if regex.match(k)]
        if self.decode_responses:
            return matched
        return [k.encode("utf-8") for k in matched]

    def scan(self, cursor=0, match=None, count=10):
        regex = None
        if match is not None:
            if isinstance(match, bytes):
                match = match.decode("utf-8")
            regex = _glob_to_regex(match)
        now = time.time()
        with self._read() as conn:
            rows = conn.execute(
                "SELECT id, key, expires_at FROM babyredis_keys"
                " WHERE id > ? ORDER BY id LIMIT ?",
                (cursor, count),
            ).fetchall()
        next_cursor = 0 if len(rows) < count else rows[-1][0]
        out = []
        for _, key, expires_at in rows:
            if expires_at is not None and expires_at <= now:
                continue
            if regex is not None and not regex.match(key):
                continue
            out.append(key if self.decode_responses else key.encode("utf-8"))
        return next_cursor, out

    def scan_iter(self, match=None, count=10):
        cursor = 0
        while True:
            cursor, page = self.scan(cursor, match=match, count=count)
            yield from page
            if cursor == 0:
                return

    def rename(self, src, dst):
        src_key = self._encode_key(src)
        dst_key = self._encode_key(dst)
        now = time.time()
        with self._write() as conn:
            row = self._lookup(conn, src_key, now)
            if row is None:
                raise ResponseError("no such key")
            if src_key == dst_key:
                return True
            dst_row = self._lookup(conn, dst_key, now)
            if dst_row is not None:
                conn.execute(
                    "DELETE FROM babyredis_keys WHERE id = ?", (dst_row[0],)
                )
            conn.execute(
                "UPDATE babyredis_keys SET key = ? WHERE id = ?",
                (dst_key, row[0]),
            )
        return True

    def renamenx(self, src, dst):
        src_key = self._encode_key(src)
        dst_key = self._encode_key(dst)
        now = time.time()
        with self._write() as conn:
            row = self._lookup(conn, src_key, now)
            if row is None:
                raise ResponseError("no such key")
            if src_key == dst_key:
                return False
            if self._lookup(conn, dst_key, now) is not None:
                return False
            conn.execute(
                "UPDATE babyredis_keys SET key = ? WHERE id = ?",
                (dst_key, row[0]),
            )
        return True

    def randomkey(self):
        now = time.time()
        with self._read() as conn:
            got = conn.execute(
                "SELECT key FROM babyredis_keys WHERE expires_at IS NULL"
                " OR expires_at > ? ORDER BY RANDOM() LIMIT 1", (now,)
            ).fetchone()
        if got is None:
            return None
        return got[0] if self.decode_responses else got[0].encode("utf-8")

    def type(self, name):
        key = self._encode_key(name)
        with self._read() as conn:
            row = self._lookup(conn, key, time.time())
        result = row[1] if row else "none"
        return result if self.decode_responses else result.encode("ascii")

    # -- pipeline -----------------------------------------------------------

    def pipeline(self, transaction=True):
        """Queue commands and run them in a single SQLite transaction.

        Unlike Redis MULTI/EXEC, the batch is fully ACID: if a queued
        command raises (and raise_on_error is True), the whole batch
        rolls back.
        """
        return Pipeline(self)

    # -- server-ish -------------------------------------------------------

    def dbsize(self):
        now = time.time()
        with self._read() as conn:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM babyredis_keys WHERE expires_at IS NULL"
                " OR expires_at > ?", (now,)
            ).fetchone()
        return count

    def flushdb(self):
        with self._write() as conn:
            conn.execute("DELETE FROM babyredis_keys")
        return True

    flushall = flushdb

    def ping(self):
        return True

    def close(self):
        with self._conns_lock:
            for conn in self._conns:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self._conns.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Pipeline:
    """Buffers client commands; execute() runs them in one transaction."""

    def __init__(self, client):
        self._client = client
        self._commands = []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        method = getattr(self._client, name)
        if not callable(method):
            raise AttributeError(name)

        def queue(*args, **kwargs):
            self._commands.append((method, args, kwargs))
            return self

        return queue

    def __len__(self):
        return len(self._commands)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.reset()

    def reset(self):
        self._commands = []

    def execute(self, raise_on_error=True):
        commands, self._commands = self._commands, []
        results = []
        with self._client._write():
            for method, args, kwargs in commands:
                try:
                    results.append(method(*args, **kwargs))
                except BabyRedisError as exc:
                    if raise_on_error:
                        raise  # _write rolls the whole batch back
                    results.append(exc)
        return results


# Drop-in friendly alias: `import babyredis; r = babyredis.Redis()`
Redis = BabyRedis
StrictRedis = BabyRedis

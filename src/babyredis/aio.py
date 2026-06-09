"""Async client shaped like redis.asyncio.

Every command is the sync implementation dispatched through
``asyncio.to_thread``, so the event loop is never blocked by SQLite I/O.
File-backed databases already use one connection per worker thread, so
the thread pool maps cleanly onto the existing concurrency model.

    from babyredis.aio import Redis

    r = Redis("cache.db")
    await r.set("k", "v", ex=60)
    await r.get("k")
    async for key in r.scan_iter(match="user:*"):
        ...
    await r.aclose()
"""

import asyncio

from babyredis.client import BabyRedis as _SyncBabyRedis


class AsyncPipeline:
    """Queues commands synchronously; execute() runs them in a thread."""

    def __init__(self, sync_pipeline):
        self._pipe = sync_pipeline

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        queue = getattr(self._pipe, name)

        def wrapper(*args, **kwargs):
            queue(*args, **kwargs)
            return self

        return wrapper

    def __len__(self):
        return len(self._pipe)

    def reset(self):
        self._pipe.reset()

    async def execute(self, raise_on_error=True):
        return await asyncio.to_thread(self._pipe.execute, raise_on_error)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.reset()


class BabyRedis:
    """Async wrapper over :class:`babyredis.BabyRedis`.

    Accepts the same arguments. All commands are awaitable; the ``*_iter``
    helpers are async generators.
    """

    def __init__(self, path="babyredis.db", **kwargs):
        self._sync = _SyncBabyRedis(path, **kwargs)

    @property
    def decode_responses(self):
        return self._sync.decode_responses

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        method = getattr(self._sync, name)
        if not callable(method):
            return method

        async def wrapper(*args, **kwargs):
            return await asyncio.to_thread(method, *args, **kwargs)

        wrapper.__name__ = name
        return wrapper

    def pipeline(self, transaction=True):
        return AsyncPipeline(self._sync.pipeline(transaction))

    async def _cursor_iter(self, scan, args, match, count):
        cursor = 0
        while True:
            cursor, page = await asyncio.to_thread(
                scan, *args, cursor, match=match, count=count)
            for item in page.items() if isinstance(page, dict) else page:
                yield item
            if cursor == 0:
                return

    def scan_iter(self, match=None, count=10):
        async def gen():
            cursor = 0
            while True:
                cursor, page = await asyncio.to_thread(
                    self._sync.scan, cursor, match=match, count=count)
                for item in page:
                    yield item
                if cursor == 0:
                    return
        return gen()

    def hscan_iter(self, name, match=None, count=10):
        return self._cursor_iter(self._sync.hscan, (name,), match, count)

    def sscan_iter(self, name, match=None, count=10):
        return self._cursor_iter(self._sync.sscan, (name,), match, count)

    def zscan_iter(self, name, match=None, count=10):
        return self._cursor_iter(self._sync.zscan, (name,), match, count)

    async def aclose(self):
        await asyncio.to_thread(self._sync.close)

    close = aclose

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()


Redis = BabyRedis
StrictRedis = BabyRedis

from __future__ import annotations

import logging
import uuid

from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class RedisLockManager:
    def __init__(self, redis_url: str, ttl_seconds: int = 120):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds
        self._tokens: dict[str, str] = {}

    async def acquire(self, stream_id: str) -> bool:
        key = f"stream_lock:{stream_id}"
        token = str(uuid.uuid4())
        ok = await self.redis.set(key, token, ex=self.ttl_seconds, nx=True)
        if ok:
            self._tokens[stream_id] = token
            return True
        logger.info("redis lock exists", extra={"stream_id": stream_id, "event": "lock"})
        return False

    async def release(self, stream_id: str) -> None:
        key = f"stream_lock:{stream_id}"
        token = self._tokens.get(stream_id)
        if not token:
            return
        script = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
"""
        try:
            await self.redis.eval(script, 1, key, token)
        finally:
            self._tokens.pop(stream_id, None)

    async def close(self) -> None:
        await self.redis.aclose()

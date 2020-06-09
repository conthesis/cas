from typing import Optional


def _cas_key(hash_: bytes) -> bytes:
    return b"dcollect_cas:" + hash_  # TODO: Change this


class Storage:
    def __init__(self, redis):
        self.redis = redis

    async def insert(self, hash_: bytes, data: bytes):
        await self.redis.set(_cas_key(hash_), data)

    async def get(self, hs: bytes) -> Optional[bytes]:
        return await self.redis.get(_cas_key(hs))

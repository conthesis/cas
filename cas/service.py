from typing import Optional

import orjson

from .normalizer import Normalizer
from .storage import Storage


class Service:
    def __init__(self, storage: Storage, normalizer: Normalizer):
        self.storage = storage
        self.normalizer = normalizer

    async def insert(self, data: bytes) -> bytes:
        (hash_, data) = self.normalizer(orjson.loads(data))
        await self.storage.insert(hash_, data)
        return hash_

    async def get(self, key: bytes) -> Optional[bytes]:
        return await self.storage.get(key)

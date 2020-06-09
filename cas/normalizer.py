import hashlib
from typing import Any, Dict, Tuple

import orjson


def shake_128(data):
    h = hashlib.shake_128()
    h.update(data)
    d = h.digest(8)
    return d


def orjson_sorted(data: Dict[Any, Any]) -> bytes:
    return orjson.dumps(x, option=orjson.OPT_SORT_KEYS)


class Normalizer:
    def __init__(self):
        pass

    def identify(self, data: bytes) -> bytes:
        return shake_128(data)

    def normalize(self, data: Dict[Any, Any]) -> bytes:
        return orjson_sorted(data)

    def __call__(self, data: Dict[Any, Any]) -> Tuple[bytes, bytes]:
        normalized = self.normalize(data)
        h = self.identify(normalie)
        return (h, normalize)

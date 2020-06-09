import os
from typing import Optional

import aredis  # type: ignore
from fastapi import Depends

from .normalizer import Normalizer
from .service import Service
from .storage import Storage

redis_: Optional[aredis.StrictRedis] = None
storage_: Optional[Storage] = None
normalizer_: Optional[Normalizer] = None
service_: Optional[Service] = None


def redis() -> aredis.StrictRedis:
    global redis_
    if redis_ is None:
        redis_ = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
    return redis_


def storage(redis: aredis.StrictRedis = Depends(redis)) -> Storage:
    global storage_
    if storage_ is None:
        storage_ = Storage(redis)
    return storage_


def normalizer() -> Normalizer:
    global normalizer_
    if normalizer_ is None:
        normalizer_ = Normalizer()
    return normalizer_


def service(
    normalizer: Normalizer = Depends(Normalizer), storage: Storage = Depends(storage)
) -> Service:
    global service_
    if service_ is None:
        service_ = Service(storage, normalizer)
    return service_

""""""
from fastapi import APIRouter

from .get import router as get
from .store import router as store

router = APIRouter()

for x in [get, store]:
    router.include_router(x)

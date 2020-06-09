""""""
import base64
from typing import Any, Dict

from fastapi import APIRouter, Depends

import cas.deps as deps

router = APIRouter()


def pointer_as_str(pointer: bytes) -> bytes:
    return base64.b64encode(pointer)


@router.post("/store")
async def store(body: Dict[str, Any], service=Depends(deps.service)) -> bytes:
    res = await service.insert(data)
    return pointer_as_str(res)

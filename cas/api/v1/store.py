""""""
from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.responses import Response

import cas.deps as deps

router = APIRouter()


@router.post("/store")
async def store(body: Dict[str, Any], service=Depends(deps.service)) -> bytes:
    res = await service.insert(body)
    return Response(content=res)

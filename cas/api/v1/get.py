""""""
import base64
from fastapi import APIRouter, Depends
from fastapi.responses import Response

import cas.deps as deps

router = APIRouter()


@router.get("/get")
async def store(p: str, service=Depends(deps.service)) -> bytes:
    res = await service.get(base64.b64decode(p))
    if res is None:
        return Response(status_code=404)
    return Response(content=res)

""""""
import base64
from fastapi import APIRouter, Depends, Body
from fastapi.responses import Response

import cas.deps as deps

router = APIRouter()


@router.post("/get")
async def store(body: bytes = Body("Keys"), service=Depends(deps.service)) -> bytes:
    res = await service.get(body)
    if res is None:
        return Response(status_code=404)
    return Response(content=res)

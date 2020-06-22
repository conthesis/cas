""""""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response

import cas.deps as deps

router = APIRouter()


@router.post("/get")
async def store(request: Request, service=Depends(deps.service)) -> bytes:
    res = await service.get(await request.body())
    if res is None:
        return Response(status_code=404)
    return Response(content=res)

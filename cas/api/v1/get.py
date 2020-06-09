""""""
from fastapi import APIRouter, Depends
from fastapi.responses import Response

import cas.deps as deps

router = APIRouter()


@router.get("/get")
async def store(p: str, service=Depends(deps.service)) -> bytes:
    res = await service.get(p)
    if res is None:
        return Response(status_code=404)
    return pointer_as_str(res)

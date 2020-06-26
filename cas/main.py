import asyncio
import os
import traceback

import aredis
from nats.aio.client import Client as NATS

from .normalizer import Normalizer
from .service import Service
from .storage import Storage

GET_TOPIC = "conthesis.cas.get"
STORE_TOPIC = "conthesis.cas.store"


async def main():
    cas = CAS()
    await cas.setup()
    await cas.wait_for_shutdown()


class CAS:
    svc: Service

    def __init__(self):
        self.nc = NATS()
        self.shutdown_f = asyncio.get_running_loop().create_future()
        redis = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
        self.svc = Service(Storage(redis), Normalizer())

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def setup(self):
        await self.nc.connect(
            os.environ.get("NATS_URL", "nats://nats:4222"),
            loop=asyncio.get_running_loop(),
        )
        self.subs = [
            await self.nc.subscribe(GET_TOPIC, cb=self.handle_get,),
            await self.nc.subscribe(STORE_TOPIC, cb=self.handle_store,),
        ]

    async def shutdown(self):
        await asyncio.gather(*[self.nc.unsubscribe(s) for s in subs])
        await self.nc.drain()

    async def reply(self, msg, data):
        reply = msg.reply
        if reply:
            await self.nc.publish(reply, data)

    async def handle_get(self, msg):
        try:
            res = await self.svc.get(msg.data)
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()

    async def handle_store(self, msg):
        try:
            res = await self.svc.insert(msg.data)
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()

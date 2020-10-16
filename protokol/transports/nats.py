import json
from typing import Callable
from nats.aio.client import Client

from protokol.transports.base import Transport


class NatsTransport(Transport):
    def __init__(self):
        self._client = Client()

    async def connect(self, url: str, **kwargs):
        return await self._client.connect(url, **kwargs)

    async def close(self):
        return await self._client.close()

    async def subscribe(self, realm: str, callback: Callable, **kwargs):
        return await self._client.subscribe(realm, cb=callback, **kwargs)

    async def publish(self, realm, message, **kwargs):
        return await self._client.publish(realm, json.dumps(message).encode())

    async def request(self, realm, message, **kwargs):
        result = await self._client.request(realm, json.dumps(message).encode(), **kwargs)
        return json.loads(result.data.decode())

    async def monitor(self, callback: Callable, **kwargs):
        return await self._client.subscribe('*', cb=callback, **kwargs)

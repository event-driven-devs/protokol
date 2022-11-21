import json
from typing import Callable

from nats.aio.client import Client
from nats.aio.errors import ErrTimeout

from protokol.transports.base import Transport


class NatsTransport(Transport):
    def __init__(self):
        self._client = Client()

    async def connect(self, *urls: str, **kwargs):
        return await self._client.connect(list(urls), **kwargs)

    async def close(self):
        return await self._client.close()

    async def subscribe(
        self, realm: str, callback: Callable, group: str = "", **kwargs
    ):
        return await self._client.subscribe(realm, cb=callback, queue=group, **kwargs)

    async def publish(self, realm, message, **kwargs):
        return await self._client.publish(realm, json.dumps(message).encode())

    async def request(self, realm, message, **kwargs):
        try:
            result = await self._client.request(
                realm, json.dumps(message).encode(), **kwargs
            )
        except ErrTimeout:
            raise TimeoutError
        return json.loads(result.data)

    async def monitor(self, callback: Callable, **kwargs):
        return await self._client.subscribe("*", cb=callback, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

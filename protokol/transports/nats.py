from typing import Any, Callable, Optional

from nats.aio.client import Client

from protokol.settings import dumps, loads
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

    async def publish(self, realm: str, message: Any, **kwargs):
        return await self._client.publish(
            subject=realm, payload=dumps(message).encode(), **kwargs
        )

    async def request(
        self, realm: str, message: Any, timeout: Optional[float] = None, **kwargs
    ):
        result = await self._client.request(
            subject=realm, payload=dumps(message).encode(), timeout=timeout, **kwargs
        )
        return loads(result.data)

    async def monitor(self, callback: Callable, **kwargs):
        return await self._client.subscribe("*", cb=callback, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

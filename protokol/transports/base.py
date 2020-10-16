from typing import Callable


class Transport:
    async def connect(self, url: str, **kwargs):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    async def subscribe(self, realm: str, callback: Callable, **kwargs):
        raise NotImplementedError

    async def publish(self, realm, message, **kwargs):
        raise NotImplementedError

    async def request(self, realm, message, **kwargs):
        raise NotImplementedError

    async def monitor(self, callback: Callable, **kwargs):
        raise NotImplementedError

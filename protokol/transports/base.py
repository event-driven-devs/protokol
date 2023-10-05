from typing import Any, Callable, Optional

from tenacity import AsyncRetrying


_sentinel = object()


class Transport:
    def __init__(self, default_retry_config: Optional[AsyncRetrying] = None):
        self._default_retry_config = default_retry_config

    async def connect(self, *urls: str, **kwargs):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    async def subscribe(
        self, realm: str, callback: Callable, group: str = None, **kwargs
    ):
        raise NotImplementedError

    async def _do_publish(self, realm: str, message: Any, **kwargs):
        raise NotImplementedError

    async def publish(
        self,
        realm: str,
        message: Any,
        retry_config: Optional[AsyncRetrying] = _sentinel,
        **kwargs
    ):
        if retry_config is _sentinel:
            retry_config = self._default_retry_config

        if retry_config:
            async for attempt in retry_config:
                with attempt:
                    return await self._do_publish(
                        realm=realm, message=message, **kwargs
                    )
        return await self._do_publish(realm=realm, message=message, **kwargs)

    async def _do_request(self, realm: str, message: Any, **kwargs):
        raise NotImplementedError

    async def request(
        self,
        realm: str,
        message: Any,
        retry_config: Optional[AsyncRetrying] = _sentinel,
        **kwargs
    ):
        if retry_config is _sentinel:
            retry_config = self._default_retry_config

        if retry_config:
            async for attempt in retry_config:
                with attempt:
                    return await self._do_request(
                        realm=realm, message=message, **kwargs
                    )
        return await self._do_request(realm=realm, message=message, **kwargs)

    async def monitor(self, callback: Callable, **kwargs):
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        """Generic property that shows whether the transport connection is
        "active" and able to receive and send data.

        Since this is a property, implementations should avoid altering
        the state     and generally stick to read-only flags available
        at request time.
        :return: whether transport is ready to receive and send data
        """
        raise NotImplementedError

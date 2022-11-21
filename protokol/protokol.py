import collections
import json
from typing import Awaitable, Callable, Iterable, List, Mapping, Optional

from protokol import settings
from protokol.logger import get_logger
from protokol.transports.base import Transport
from protokol.transports.nats import NatsTransport

logger = get_logger("protokol")


class CallException(Exception):
    pass


class Protokol:
    def __init__(self, *args, **kwargs):
        self._url = None
        self._transport = None
        self._connection_args = []
        self._connection_kwargs = {}

    @property
    def connection_args(self) -> List:
        return self._connection_args

    @connection_args.setter
    def connection_args(self, value: Iterable):
        """
            For new connection settings to take action, call `self.connect(force=True)`
        :param value:
        :return:
        """
        self._connection_args = list(value or [])

    @property
    def connection_kwargs(self) -> dict:
        return self._connection_kwargs

    @connection_kwargs.setter
    def connection_kwargs(self, value: Mapping):
        """
            For new connection settings to take action, call `self.connect(force=True)`
        :param value:
        :return:
        """
        self._connection_kwargs = dict(value or {})

    @property
    def is_connected(self) -> bool:
        return self._transport.is_connected

    async def _start(self):
        await self._transport.connect(
            self._url, *self.connection_args, **self.connection_kwargs
        )
        logger.info("Connected to {}".format(self._url))
        await self._start_listeners()
        logger.info("Started listeners")

    async def connect(self, force: bool = False):
        if self.is_connected:
            if not force:
                logger.warning(
                    "Already connected to %s. "
                    "Use `force` argument to reconnect anyway.",
                    self._url,
                )
                return
            await self.close()
        return await self._start()

    async def close(self):
        if self.is_connected:
            try:
                await self._transport.close()
            except Exception as e:
                # In case library client misreports the 'connected' status
                logger.warning(
                    f"Failed to close transport."
                    f"{f'Error: {e}' if not settings.DEBUG else ''}",
                    exc_info=True,
                )

    @classmethod
    async def create(
        cls,
        mq_url: str,
        *args,
        transport: Transport = None,
        connection_args: Iterable = None,
        connection_kwargs: Mapping = None,
        **kwargs,
    ):
        self = cls(*args, **kwargs)
        self._url = mq_url
        self._transport = transport or NatsTransport()
        self.connection_args = connection_args
        self.connection_kwargs = connection_kwargs
        await self.connect(force=True)
        return self

    async def _start_listeners(self):
        for attr in self.__class__.__dict__.values():
            if (
                hasattr(attr, "__qualname__")
                and "Protokol.listener" in attr.__qualname__
            ):
                await attr(self)
            if (
                hasattr(attr, "__qualname__")
                and "Protokol.callable" in attr.__qualname__
            ):
                await attr(self)
            if (
                hasattr(attr, "__qualname__")
                and "Protokol.monitor" in attr.__qualname__
            ):
                await attr(self)

    def _is_my_method(self, function: Callable):
        return (
            hasattr(function, "__name__")
            and hasattr(self, function.__name__)
            and (
                hasattr(function, "__call__")
                or isinstance(function, collections.Callable)
            )
        )

    async def make_listener(
        self,
        realm: str,
        signal_name: Optional[str],
        group: Optional[str],
        handler: Callable[..., Awaitable],
        loopback_allowed: bool = False,
    ):
        async def signal_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(
                    "Exception in {}.{} in JSON deserialization".format(
                        realm, signal_name
                    ),
                    exc_info=True,
                )
                return
            signal = data.get("signal", "")
            is_not_signal = "signal" not in data
            is_disallowed_loopback = not loopback_allowed and id(self) == data.get("id")
            is_not_my_signal_name = signal_name is not None and signal != signal_name
            if is_disallowed_loopback or is_not_my_signal_name or is_not_signal:
                return
            args = data.get("args", [])
            kwargs = data.get("kwargs", {})
            logger.debug("<< Got signal: {}, {}".format(realm, signal))
            logger.debug("   args: {}".format(args))
            logger.debug("   kwargs: {}".format(kwargs))
            if signal_name is None:
                kwargs["_signal_name"] = signal
            try:
                if self._is_my_method(handler):
                    args = [self, *args]
                return await handler(*args, **kwargs)
            except Exception:
                logger.error(
                    "Exception in {}.{} signal handler".format(realm, signal),
                    exc_info=True,
                )

        logger.debug(
            "Make listener: {} {} {} {}".format(realm, signal_name, group, handler)
        )
        await self._transport.subscribe(realm, group=group, callback=signal_handler)

    async def make_callable(
        self,
        realm: str,
        function_name: Optional[str],
        handler: Callable[..., Awaitable],
        loopback_allowed: bool = False,
    ):
        async def call_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(
                    "Exception in {}.{} in JSON deserialization".format(
                        realm, function_name
                    ),
                    exc_info=True,
                )
                return
            called = data.get("invoke", "")
            is_not_call = "invoke" not in data
            is_disallowed_loopback = not loopback_allowed and id(self) == data.get("id")
            is_not_my_function_name = (
                function_name is not None and called != function_name
            )
            if is_disallowed_loopback or is_not_my_function_name or is_not_call:
                return
            args = data.get("args", [])
            kwargs = data.get("kwargs", {})
            logger.debug("<< Got call: {}, {}".format(realm, called))
            logger.debug("   args: {}".format(args))
            logger.debug("   kwargs: {}".format(kwargs))
            if function_name is None:
                args = [called, *args]
            try:
                if self._is_my_method(handler):
                    args = [self, *args]
                result_data = await handler(*args, **kwargs)
                result = {"status": "ok", "result": result_data}
            except Exception as e:
                logger.error(
                    "Exception in {}.{} function handler".format(realm, called),
                    exc_info=True,
                )
                result = {"status": "error", "result": str(e)}
            try:
                await self._transport.publish(msg.reply, result)
            except Exception:
                logger.error(
                    "Exception in {}.{} function handler on result send".format(
                        realm, called
                    ),
                    exc_info=True,
                )
            logger.debug(">> Send result: {}".format(result))

        logger.debug("Make callable: {} {} {}".format(realm, function_name, handler))
        await self._transport.subscribe(realm, callback=call_handler)

    async def make_monitor(self, name: str, handler: Callable[..., Awaitable]):
        async def monitor_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(
                    "Exception in JSON deserialization in {} monitor".format(name),
                    exc_info=True,
                )
                return None
            try:
                args = [msg.subject, data]
                if self._is_my_method(handler):
                    args = [self, args]
                return await handler(*args)
            except Exception:
                logger.error(
                    "Exception in {} monitor handler".format(name), exc_info=True
                )

        logger.debug("Make monitor {}: {}".format(name, handler))
        await self._transport.monitor(callback=monitor_handler)

    async def emit(self, realm: str, signal_name: str, *args, **kwargs):
        signal_data = {
            "signal": signal_name,
            "args": args,
            "kwargs": kwargs,
            "id": id(self),
        }
        logger.debug(">> Send signal: {}, {}".format(realm, signal_name))
        logger.debug("   args: {}".format(args))
        logger.debug("   kwargs: {}".format(kwargs))
        await self._transport.publish(realm, signal_data)

    async def call(self, realm: str, function_name: str, *args, **kwargs):
        logger.debug(">> Send call: {}, {}".format(realm, function_name))
        logger.debug("   args: {}".format(args))
        logger.debug("   kwargs: {}".format(kwargs))
        call_data = {
            "invoke": function_name,
            "args": args,
            "kwargs": kwargs,
            "id": id(self),
        }
        reply = await self._transport.request(
            realm, call_data, timeout=settings.CALL_TIMEOUT
        )
        logger.debug("<< Got result: {}".format(reply))
        status = reply.get("status")
        result = reply.get("result")
        if status == "ok":
            return result
        elif status == "error":
            raise CallException(result)
        raise CallException("Internal error: bad reply from remote site")

    @classmethod
    def listener(
        cls,
        realm: str,
        signal_name: str = None,
        group: str = "",
        loopback_allowed: bool = False,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_listener(
                    realm,
                    signal_name,
                    group=group,
                    handler=func,
                    loopback_allowed=loopback_allowed,
                )

            return wrapper

        return inner_function

    @classmethod
    def callable(cls, realm: str, function_name: str = None, loopback_allowed=False):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_callable(
                    realm,
                    function_name,
                    handler=func,
                    loopback_allowed=loopback_allowed,
                )

            return wrapper

        return inner_function

    @classmethod
    def signal(cls, realm: str, signal_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.emit(realm, signal_name, *args, **kwargs)
                return await func(self, *args, **kwargs)

            return wrapper

        return inner_function

    @classmethod
    def caller(cls, realm: str, function_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await func(self, *args, **kwargs)
                return await self.call(realm, function_name, *args, **kwargs)

            return wrapper

        return inner_function

    @classmethod
    def monitor(cls, name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_monitor(name, handler=func)

            return wrapper

        return inner_function

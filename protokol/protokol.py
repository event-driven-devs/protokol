import collections
import json
from typing import AsyncIterator, Awaitable, Callable, Iterable, List, Mapping, Optional

from tenacity import AsyncRetrying, AttemptManager, stop_after_attempt

from protokol import settings
from protokol.logger import get_logger
from protokol.transports.base import Transport
from protokol.transports.nats import NatsTransport

logger = get_logger("protokol")


_sentinel = object()


class CallError(Exception):
    pass


CallException = CallError


class Protokol:
    def __init__(self):
        self._url = None
        self._transport = None
        self._connection_args = []
        self._connection_kwargs = {}
        self._default_retry_config = None

    @property
    def connection_args(self) -> List:
        return self._connection_args

    @connection_args.setter
    def connection_args(self, value: Iterable):
        """For new connection settings to take action, call
        `self.connect(force=True)` :param value:

        :return:
        """
        self._connection_args = list(value or [])

    @property
    def connection_kwargs(self) -> dict:
        return self._connection_kwargs

    @connection_kwargs.setter
    def connection_kwargs(self, value: Mapping):
        """For new connection settings to take action, call
        `self.connect(force=True)` :param value:

        :return:
        """
        self._connection_kwargs = dict(value or {})

    async def _retrying(
        self, retry_config: Optional[AsyncRetrying] = _sentinel
    ) -> AsyncIterator[AttemptManager]:
        """Wrap an arbitrary code with this iterator to add retry logic to it.
        Handling logic is dictated by the specific retry config supplied to the
        method or the config from Protokol instance (if either exists).

        :param retry_config: Custom retry config. If not supplied, the
            instance default will be used, if present. If no configs are
            supplied, this is just a noop
        :return:
        """
        if retry_config is _sentinel:
            retry_config = self._default_retry_config

        if retry_config is None:
            retry_config = AsyncRetrying(stop=stop_after_attempt(1), reraise=True)

        async for attempt in retry_config:
            # TODO: Using `with` here doesn't really work
            #  (attemptManager catches asyncio.CancelledError instead of the one originally raised)
            yield attempt

    @property
    def is_connected(self) -> bool:
        return self._transport.is_connected

    async def _start(self):
        # TODO: Apply retry config here as well?
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
                return None
            await self.close()
        return await self._start()

    async def close(self):
        if self.is_connected:
            try:
                await self._transport.close()
            except Exception as e:  # noqa: BLE001
                # In case library client misreports the 'connected' status
                logger.warning(
                    f"Failed to close transport."
                    f"{f'Error: {e}' if not settings.DEBUG else ''}",
                    exc_info=True,
                )

    @classmethod
    async def create(  # noqa: PLR0913
        cls,
        mq_url: str,
        *args,
        transport: Transport = None,
        connection_args: Optional[Iterable] = None,
        connection_kwargs: Optional[Mapping] = None,
        default_retry_config: Optional[AsyncRetrying] = None,
        **kwargs,
    ):
        self = cls(*args, **kwargs)
        self._url = mq_url
        self._transport = transport or NatsTransport()
        self.connection_args = connection_args
        self.connection_kwargs = connection_kwargs
        self._default_retry_config = default_retry_config
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
            and (callable(function) or isinstance(function, collections.Callable))
        )

    async def make_listener(  # noqa: PLR0913
        self,
        realm: str,
        signal_name: Optional[str],
        group: Optional[str],
        handler: Callable[..., Awaitable],
        loopback_allowed: bool = False,
        retry_config: Optional[AsyncRetrying] = _sentinel,
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
                return None
            signal = data.get("signal", "")
            is_not_signal = "signal" not in data
            is_disallowed_loopback = not loopback_allowed and id(self) == data.get("id")
            is_not_my_signal_name = signal_name is not None and signal != signal_name
            if is_disallowed_loopback or is_not_my_signal_name or is_not_signal:
                return None
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

        async for attempt in self._retrying(retry_config=retry_config):
            with attempt:
                await self._transport.subscribe(
                    realm, group=group, callback=signal_handler
                )

    async def make_callable(  # noqa: PLR0913
        self,
        realm: str,
        function_name: Optional[str],
        handler: Callable[..., Awaitable],
        loopback_allowed: bool = False,
        retry_config: Optional[AsyncRetrying] = _sentinel,
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
        async for attempt in self._retrying(retry_config=retry_config):
            with attempt:
                await self._transport.subscribe(realm, callback=call_handler)

    async def make_monitor(
        self,
        name: str,
        handler: Callable[..., Awaitable],
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
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
        async for attempt in self._retrying(retry_config=retry_config):
            with attempt:
                await self._transport.monitor(callback=monitor_handler)

    async def emit(
        self,
        realm: str,
        signal_name: str,
        *args,
        retry_config: Optional[AsyncRetrying] = _sentinel,
        **kwargs,
    ):
        signal_data = {
            "signal": signal_name,
            "args": args,
            "kwargs": kwargs,
            "id": id(self),
        }
        logger.debug(">> Send signal: {}, {}".format(realm, signal_name))
        logger.debug("   args: {}".format(args))
        logger.debug("   kwargs: {}".format(kwargs))
        async for attempt in self._retrying(retry_config=retry_config):
            with attempt:
                await self._transport.publish(realm, signal_data)

    async def call(
        self,
        realm: str,
        function_name: str,
        *args,
        retry_config: Optional[AsyncRetrying] = _sentinel,
        **kwargs,
    ):
        logger.debug(">> Send call: {}, {}".format(realm, function_name))
        logger.debug("   args: {}".format(args))
        logger.debug("   kwargs: {}".format(kwargs))
        call_data = {
            "invoke": function_name,
            "args": args,
            "kwargs": kwargs,
            "id": id(self),
        }

        async for attempt in self._retrying(retry_config):
            with attempt:
                reply = await self._transport.request(
                    realm, call_data, timeout=settings.CALL_TIMEOUT
                )

        logger.debug("<< Got result: {}".format(reply))
        status = reply.get("status")
        result = reply.get("result")
        if status == "ok":
            return result
        if status == "error":
            raise CallException(result)
        raise CallException("Internal error: bad reply from remote site")

    @classmethod
    def listener(  # noqa: PLR0913
        cls,
        realm: str,
        signal_name: Optional[str] = None,
        group: str = "",
        loopback_allowed: bool = False,
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol):
                if not isinstance(self, Protokol):
                    raise ValueError
                await self.make_listener(
                    realm,
                    signal_name,
                    group=group,
                    handler=func,
                    loopback_allowed=loopback_allowed,
                    retry_config=retry_config,
                )

            return wrapper

        return inner_function

    @classmethod
    def callable(  # noqa: A003
        cls,
        realm: str,
        function_name: Optional[str] = None,
        loopback_allowed: bool = False,
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol):
                if not isinstance(self, Protokol):
                    raise ValueError
                await self.make_callable(
                    realm,
                    function_name,
                    handler=func,
                    loopback_allowed=loopback_allowed,
                    retry_config=retry_config,
                )

            return wrapper

        return inner_function

    @classmethod
    def signal(
        cls,
        realm: str,
        signal_name: str,
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError
                await self.emit(
                    realm,
                    signal_name,
                    *args,
                    retry_config=retry_config,
                    **kwargs,
                )
                return await func(self, *args, **kwargs)

            return wrapper

        return inner_function

    @classmethod
    def caller(
        cls,
        realm: str,
        function_name: str,
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, *args, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError
                await func(self, *args, **kwargs)
                return await self.call(
                    realm,
                    function_name,
                    *args,
                    retry_config=retry_config,
                    **kwargs,
                )

            return wrapper

        return inner_function

    @classmethod
    def monitor(
        cls,
        name: str,
        retry_config: Optional[AsyncRetrying] = _sentinel,
    ):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol):
                if not isinstance(self, Protokol):
                    raise ValueError
                await self.make_monitor(
                    name=name, handler=func, retry_config=retry_config
                )

            return wrapper

        return inner_function

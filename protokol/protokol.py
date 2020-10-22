import collections
import json
from typing import Callable

from protokol import settings

from protokol.logger import get_logger
from protokol.transports.base import Transport
from protokol.transports.nats import NatsTransport

logger = get_logger('protokol')


class CallException(Exception):
    pass


class Protokol:
    @classmethod
    async def create(cls, mq_url: str, *args, transport: Transport = NatsTransport(), **kwargs):
        self = cls(*args, **kwargs)
        await self._init(mq_url, transport)
        return self

    async def _init(self, mq_url: str, transport: Transport):
        self._transport = transport
        await self._transport.connect(mq_url)
        logger.info(f"Connected to {mq_url}")
        await self._start_listeners()

    async def close(self):
        await self._transport.close()

    async def _start_listeners(self):
        for attr in self.__class__.__dict__.values():
            if hasattr(attr, '__qualname__') and 'Protokol.listener' in attr.__qualname__:
                await attr(self)
            if hasattr(attr, '__qualname__') and 'Protokol.callable' in attr.__qualname__:
                await attr(self)
            if hasattr(attr, '__qualname__') and 'Protokol.monitor' in attr.__qualname__:
                await attr(self)

    def _is_my_method(self, function: Callable):
        return hasattr(function, '__name__') and hasattr(self, function.__name__) and (
                hasattr(function, '__call__') or isinstance(function, collections.Callable)
        )

    async def make_listener(self, realm: str, signal_name: str, handler: Callable):
        async def signal_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(f'Exception in {realm}.{signal_name} in JSON deserialization', exc_info=True)
                return
            signal = data.get('signal', '')
            if signal != signal_name:
                return
            arguments = data.get('args', {})
            logger.debug(f'<< Got signal: {realm}, {signal}')
            logger.debug(f'   Args: {arguments}')
            try:
                return await handler(self, **arguments) if self._is_my_method(handler) else await handler(**arguments)
            except Exception:
                logger.error(f'Exception in {realm}.{signal} signal handler', exc_info=True)

        logger.debug(f'Make listener: {realm} {signal_name} {handler}')
        await self._transport.subscribe(realm, callback=signal_handler)

    async def make_callable(self, realm: str, function_name: str, func: Callable):
        async def call_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(f'Exception in {realm}.{function_name} in JSON deserialization', exc_info=True)
                return
            called = data.get('invoke', '')
            if called != function_name:
                return
            arguments = data.get('args', {})
            logger.debug(f'<< Got call: {realm}, {called}')
            logger.debug(f'   Args: {arguments}')
            try:
                result_data = await func(self, **arguments) if self._is_my_method(func) else await func(**arguments)
                result = {'status': 'ok', 'result': result_data}
            except Exception as e:
                logger.error(f'Exception in {realm}.{called} function handler', exc_info=True)
                result = {'status': 'error', 'result': str(e)}
            try:
                await self._transport.publish(msg.reply, result)
            except Exception as e:
                logger.error(f'Exception in {realm}.{called} function handler on result send', exc_info=True)
            logger.debug(f'>> Send result: {result}')

        logger.debug(f'Make callable: {realm} {function_name} {func}')
        await self._transport.subscribe(realm, callback=call_handler)

    async def make_monitor(self, name: str, func: Callable):
        async def monitor_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error(f'Exception in JSON deserialization in {name} monitor', exc_info=True)
                return None
            try:
                return await func(self, msg.subject, data) if self._is_my_method(func) else await func(msg.subject, data)
            except Exception:
                logger.error(f'Exception in {name} monitor handler', exc_info=True)

        logger.debug(f'Make monitor {name}: {func}')
        await self._transport.monitor(callback=monitor_handler)

    async def emit(self, realm: str, signal_name: str, **kwargs):
        signal_data = {
            'signal': signal_name,
            'args': kwargs
        }
        logger.debug(f'>> Send signal: {signal_name}, {kwargs}')
        await self._transport.publish(realm, signal_data)

    async def call(self, realm: str, function_name: str, **kwargs):
        logger.debug(f'>> Send call: {realm}, {function_name}, {kwargs}')
        call_data = {
            'invoke': function_name,
            'args': kwargs
        }
        reply = await self._transport.request(realm, call_data, timeout=settings.CALL_TIMEOUT)
        logger.debug(f'<< Got result: {reply}')
        status = reply.get('status')
        result = reply.get('result')
        if status == 'ok':
            return result
        elif status == 'error':
            raise CallException(result)
        raise CallException('Internal error: bad reply from remote site')

    @classmethod
    def listener(cls, realm: str, signal_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_listener(realm, signal_name, func)
            return wrapper
        return inner_function

    @classmethod
    def callable(cls, realm: str, function_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_callable(realm, function_name, func)
            return wrapper
        return inner_function

    @classmethod
    def signal(cls, realm: str, signal_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.emit(realm, signal_name, **kwargs)
                return await func(self, **kwargs)
            return wrapper
        return inner_function

    @classmethod
    def caller(cls, realm: str, function_name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await func(self, **kwargs)
                return await self.call(realm, function_name, **kwargs)
            return wrapper

        return inner_function

    @classmethod
    def monitor(cls, name: str):
        def inner_function(func: Callable):
            async def wrapper(self: Protokol, **kwargs):
                if not isinstance(self, Protokol):
                    raise ValueError()
                await self.make_monitor(name, func)
            return wrapper
        return inner_function

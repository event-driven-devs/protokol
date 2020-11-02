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
        logger.info('Connected to {}'.format(mq_url))
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
                logger.error('Exception in {}.{} in JSON deserialization'.format(realm, signal_name), exc_info=True)
                return
            signal = data.get('signal', '')
            if signal_name != '*' and signal != signal_name:
                return
            arguments = data.get('args', {})
            logger.debug('<< Got signal: {}, {}'.format(realm, signal))
            logger.debug('   Args: {}'.format(arguments))
            try:
                return await handler(self, **arguments) if self._is_my_method(handler) else await handler(**arguments)
            except Exception:
                logger.error('Exception in {}.{} signal handler'.format(realm, signal), exc_info=True)

        logger.debug('Make listener: {} {} {}'.format(realm, signal_name, handler))
        await self._transport.subscribe(realm, callback=signal_handler)

    async def make_callable(self, realm: str, function_name: str, func: Callable):
        async def call_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error('Exception in {}.{} in JSON deserialization'.format(realm, function_name), exc_info=True)
                return
            called = data.get('invoke', '')
            if function_name != '*' and called != function_name:
                return
            arguments = data.get('args', {})
            logger.debug('<< Got call: {}, {}'.format(realm, called))
            logger.debug('   Args: {}'.format(arguments))
            try:
                result_data = await func(self, **arguments) if self._is_my_method(func) else await func(**arguments)
                result = {'status': 'ok', 'result': result_data}
            except Exception as e:
                logger.error('Exception in {}.{} function handler'.format(realm, called), exc_info=True)
                result = {'status': 'error', 'result': str(e)}
            try:
                await self._transport.publish(msg.reply, result)
            except Exception as e:
                logger.error('Exception in {}.{} function handler on result send'.format(realm, called), exc_info=True)
            logger.debug('>> Send result: {}'.format(result))

        logger.debug('Make callable: {} {} {}'.format(realm, function_name, func))
        await self._transport.subscribe(realm, callback=call_handler)

    async def make_monitor(self, name: str, func: Callable):
        async def monitor_handler(msg):
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                logger.error('Exception in JSON deserialization in {} monitor'.format(name), exc_info=True)
                return None
            try:
                return await func(self, msg.subject, data) if self._is_my_method(func) else await func(msg.subject, data)
            except Exception:
                logger.error('Exception in {} monitor handler'.format(name), exc_info=True)

        logger.debug('Make monitor {}: {}'.format(name, func))
        await self._transport.monitor(callback=monitor_handler)

    async def emit(self, realm: str, signal_name: str, **kwargs):
        signal_data = {
            'signal': signal_name,
            'args': kwargs
        }
        logger.debug('>> Send signal: {}, {}'.format(signal_name, kwargs))
        await self._transport.publish(realm, signal_data)

    async def call(self, realm: str, function_name: str, **kwargs):
        logger.debug('>> Send call: {}, {}, {}'.format(realm, function_name, kwargs))
        call_data = {
            'invoke': function_name,
            'args': kwargs
        }
        reply = await self._transport.request(realm, call_data, timeout=settings.CALL_TIMEOUT)
        logger.debug('<< Got result: {}'.format(reply))
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

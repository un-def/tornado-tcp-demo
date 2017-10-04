import time
import struct
import logging
import operator
from enum import IntEnum
from functools import reduce
from collections import namedtuple, OrderedDict

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer


logger = logging.getLogger(__name__)


class ParseError(Exception):

    pass


class BaseConnection:

    READ_CHUNK_SIZE = 0x10000

    def __init__(self, stream):
        self.stream = stream

    async def read(self):
        return await self.stream.read_bytes(self.READ_CHUNK_SIZE, partial=True)

    async def write(self, data):
        await self.stream.write(data)


class Source(BaseConnection):

    HEADER_SIZE = 13
    HEADER_STRUCT = '<BH8sBB'
    HEADER_MARKER = 0x01
    FIELD_SIZE = 12
    FIELD_STRUCT = '<8sL'
    RESPONSE_STRUCT = '<BHB'

    Header = namedtuple('Header', 'marker msgno srcid status numfields')

    class Status(IntEnum):
        IDLE = 0x01
        ACTIVE = 0x02
        RECHARGE = 0x03

    class ResponseHeader(IntEnum):
        OK = 0x11
        PARSE_ERROR = 0x12

    def __init__(self, *args, new_message_hook=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.new_message_hook = new_message_hook
        self.last_seen = None

    async def serve(self):
        while True:
            try:
                await self.communicate()
            except StreamClosedError:
                logger.debug('SOURCE connection closed.')
                break

    async def communicate(self):
        message = await self.read()
        try:
            header = self.parse_header(message[:self.HEADER_SIZE])
        except ParseError as exc:
            logger.debug('Header parse error: %s', exc)
            await self.respond_parse_error()
            return
        fields_size = self.FIELD_SIZE * header.numfields
        if len(message) != self.HEADER_SIZE + fields_size + 1:
            logger.debug('Wrong message size.')
            await self.respond_parse_error()
            return
        check_byte = self.calculate_xor(message[:-1])
        if check_byte != message[-1]:
            logger.debug('Invalid check byte.')
            await self.respond_parse_error()
            return
        try:
            fields = self.parse_fields(message[self.HEADER_SIZE:-1])
        except ParseError as exc:
            logger.debug('Fields parse error: %s', exc)
            await self.respond_parse_error()
            return
        logger.debug('New message from %s.', header.srcid)
        self.last_header = header
        self.last_seen = time.time()
        await self.respond_ok(header.msgno)
        if self.new_message_hook:
            await self.new_message_hook(header, fields)

    async def respond_ok(self, msgno):
        await self._respond(self.ResponseHeader.OK, msgno)

    async def respond_parse_error(self):
        await self._respond(self.ResponseHeader.PARSE_ERROR, 0)

    async def _respond(self, header, msgno):
        response = struct.pack(self.RESPONSE_STRUCT, header, msgno, 0)
        response_check_byte = self.calculate_xor(response)
        response = response[:-1] + response_check_byte.to_bytes(1, 'little')
        await self.write(response)

    @classmethod
    def parse_header(cls, header_bytes):
        """Parse message header to Header namedtuple"""
        try:
            header_list = list(struct.unpack(cls.HEADER_STRUCT, header_bytes))
        except (TypeError, struct.error) as exc:
            raise ParseError(str(exc)) from exc
        if header_list[0] != cls.HEADER_MARKER:
            raise ParseError('Invalid source marker.')
        if header_list[3] not in cls.Status.__members__.values():
            raise ParseError('Invalid source status.')
        try:
            srcid = cls.decode_ascii(header_list[2])
        except ValueError as exc:
            raise ParseError(str(exc)) from exc
        header_list[2] = srcid
        return cls.Header(*header_list)

    @classmethod
    def parse_fields(cls, fields_bytes):
        """Parse fields part of message to OrderedDict {field_name: value}"""
        fields = OrderedDict()
        for offset in range(0, len(fields_bytes), cls.FIELD_SIZE):
            field_bytes = fields_bytes[offset:offset+cls.FIELD_SIZE]
            try:
                name, value = struct.unpack(cls.FIELD_STRUCT, field_bytes)
            except struct.error as exc:
                raise ParseError(str(exc)) from exc
            try:
                name = cls.decode_ascii(name)
            except ValueError as exc:
                raise ParseError(str(exc)) from exc
            fields[name] = value
        return fields

    @staticmethod
    def decode_ascii(byte_string):
        """Decode ASCII bytes to str object"""
        byte_string = byte_string.rstrip(b' \x00')
        if any(map(lambda c: c < 32 or c > 126, byte_string)):
            raise ValueError('Invalid ASCII symbol.')
        return byte_string.decode('ascii')

    @staticmethod
    def calculate_xor(data_bytes):
        """Calculate byte-to-byte XOR value of given byte sequence"""
        return reduce(operator.xor, data_bytes)


class Listener(BaseConnection):

    async def notify_on_connect(self, sources):
        now = time.time()
        info_list = []
        for source in sources:
            if not source.last_seen:
                continue
            past = int((now - source.last_seen) * 1000)
            header = source.last_header
            status = Source.Status(header.status).name
            info_list.append(f'[{header.srcid}] {header.msgno} '
                             f'| {status} | {past}\r\n')
        info = ''.join(info_list)
        if not info:
            return True
        try:
            await self.write(info.encode('ascii'))
            return True
        except StreamClosedError:
            return False

    async def notify_on_new_message(self, header, fields):
        if not fields:
            return True
        info_gen = (f'[{header.srcid}] {field} | {value}\r\n'
                    for field, value in fields.items())
        info = ''.join(info_gen)
        try:
            await self.write(info.encode('ascii'))
            return True
        except StreamClosedError:
            return False


class MessageServer(TCPServer):

    def __init__(self, *args, source_port=None, listener_port=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_port = source_port
        self.listener_port = listener_port
        if source_port:
            self.bind(source_port)
        if listener_port:
            self.bind(listener_port)
        self.sources = set()
        self.listeners = set()

    async def handle_stream(self, stream, address):
        sock = stream.fileno()
        port = sock.getsockname()[1]
        if port == self.source_port:
            logger.debug('New SOURCE connection.')
            source = Source(stream, new_message_hook=self.notify_listeners)
            self.sources.add(source)
            await source.serve()
            self.sources.remove(source)
        else:
            logger.debug('New LISTENER connection.')
            listener = Listener(stream)
            ok = await listener.notify_on_connect(self.sources)
            if ok:
                self.listeners.add(listener)
            else:
                logger.debug('LISTENER connection closed.')

    async def notify_listeners(self, header, fields):
        for listener in list(self.listeners):
            ok = await listener.notify_on_new_message(header, fields)
            if not ok:
                logger.debug('LISTENER connection closed.')
                self.listeners.remove(listener)


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    server = MessageServer(source_port=8888, listener_port=8889)
    server.start()
    IOLoop.current().start()

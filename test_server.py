import re
import socket
import struct

import pytest
from tornado.tcpclient import TCPClient
from tornado import gen

from server import MessageServer, Source


def _create_tcp_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    return sock


@pytest.fixture(scope='session')
def unused_tcp_ports():
    """Get two random free TCP ports"""
    sock1 = _create_tcp_socket()
    sock2 = _create_tcp_socket()
    port1 = sock1.getsockname()[1]
    port2 = sock2.getsockname()[1]
    sock1.close()
    sock2.close()
    return port1, port2


@pytest.fixture(scope='session')
def source_port(unused_tcp_ports):
    return unused_tcp_ports[0]


@pytest.fixture(scope='session')
def listener_port(unused_tcp_ports):
    return unused_tcp_ports[1]


@pytest.fixture
def server(io_loop, source_port, listener_port):
    server = MessageServer(source_port=source_port,
                           listener_port=listener_port)
    server.start()
    yield server
    server.stop()


class TCPClientStream:

    """Async context manager for Tornado TCP connection factory"""

    def __init__(self, port):
        self.port = port

    async def __aenter__(self):
        client = TCPClient()
        stream = await client.connect('127.0.0.1', self.port)
        self.stream = stream
        return self.stream

    async def __aexit__(self, exc_type, exc, tb):
        self.stream.close()


@pytest.mark.parametrize('message,expected', [
    # OK
    (b'\x01\x01\x00foo\x00\x00\x00\x00\x00\x02\x00d', b'\x11\x01\x00\x10'),
    (b'\x01\x01\x00foo     \x02\x00D', b'\x11\x01\x00\x10'),
    # PARSE_ERROR
    (b'\x00', b'\x12\x00\x00\x12'),
    (b'\x01\x01\x00foo\x00\x00\x00\x00\x00\x04\x00b', b'\x12\x00\x00\x12'),
    (b'\x01\x01\x00\x00oo\x00\x00\x00\x00\x00\x02\x00e', b'\x12\x00\x00\x12'),
    (b'\x01\x01\x00foo\x00\x00\x00\x00\x00\x02\x00e', b'\x12\x00\x00\x12'),
    (b'\x02\x01\x00foo\x00\x00\x00\x00\x00\x02\x00d', b'\x12\x00\x00\x12'),
])
async def test_source(server, source_port, message, expected):
    async with TCPClientStream(source_port) as stream:
        await stream.write(message)
        response = await stream.read_bytes(1024, partial=True)
        assert response == expected


async def test_listener(server, source_port, listener_port):
    message = (b'\x01\x05\x00foobar  \x02\x03SPEED   \x10\x20\x00\x00'
               b'RPM     \x05\x00\x00\x00POWER   \xff\x00\x00\x00\xaf')
    async with TCPClientStream(source_port) as source_stream:
        await source_stream.write(message)
        await gen.sleep(.1)
        async with TCPClientStream(listener_port) as listener_stream:
            response = await listener_stream.read_bytes(1024, partial=True)
            assert re.match(rb'^\[foobar\] 5 | ACTIVE | \d+$', response, re.M)
            await source_stream.write(message)
            response = await listener_stream.read_bytes(1024, partial=True)
            assert re.match(rb'^\[foobar\] [A-Z]+ | \d+$', response, re.M)


def test_source_parse_header():
    buf = struct.pack(Source.HEADER_STRUCT, 1, 1, b'foo', 2, 1)
    header = Source.parse_header(buf)
    assert header.msgno == 1
    assert header.srcid == 'foo'

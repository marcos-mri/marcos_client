#!/usr/bin/env python3

import msgpack, warnings
from collections.abc import Iterator
from socket import socket as _Socket
from typing import Any

from marmachine import MarServerWarning

version_major = 1
version_minor = 0
version_debug = 6
version_full = (version_major << 16) | (version_minor << 8) | version_debug

request_pkt = 0
emergency_stop_pkt = 1
close_server_pkt = 2
reply_pkt = 128
reply_error_pkt = 129

def _is_reply(msg):
    """True if the message is a final reply (as opposed to an intermediate stream message)."""
    return msg[0] >= reply_pkt

def construct_packet(data, packet_idx=0, command=request_pkt, version=(version_major, version_minor, version_debug), params=None):
    vma, vmi, vd = version
    assert vma < 256 and vmi < 256 and vd < 256, "Version is too high for a byte!"
    version = (vma << 16) | (vmi << 8) | vd
    fields = [command, packet_idx, params if params is not None else 0, version, data]
    return fields

# def process(payload, print_all=False):
#     # data = msgpack.unpackb(raw_reply, use_list=False, max_array_len=1024*1024)
#     reply_data = payload[4]

#     if print_all:
#         print("")

#         status = payload[5]

#         try:
#             print("Errors:")
#             for k in status['errors']:
#                 print(k)
#         except KeyError:
#             pass

#         try:
#             print("Warnings:")
#             for k in status['warnings']:
#                 print(k)
#         except KeyError:
#             pass

#         try:
#             print("Infos:")
#             for k in status['infos']:
#                 print(k)
#         except KeyError:
#             pass

#     try:
#         print("Last elements of returned unsigned arrays: {:f}, {:f}".format(
#             payload[4]['test_throughput']['array1'][-1], payload[4]['test_throughput']['array2'][-1]))
#     except KeyError:
#         print("Reply data: ")
#         print(reply_data)

def receive_response(socket):
    """Yield each decoded message from the server.
    Intermediate (streaming) messages are yielded as-is.
    Iteration stops after the final reply message."""
    unpacker = msgpack.Unpacker()
    while True:
        buf = socket.recv(65536)
        if not buf:
            raise ConnectionError("server closed connection before sending the final reply")
        unpacker.feed(buf)
        for msg in unpacker:
            yield msg
            if _is_reply(msg):
                trailing = next(unpacker, None)
                assert trailing is None, "unexpected data after final reply"
                return

def send_packet(packet, socket):
    socket.sendall(msgpack.packb(packet))

    reply = None
    for msg in receive_response(socket):
        if _is_reply(msg):
            reply = msg
        else:
            raise RuntimeError(f"unexpected intermediate message from server: {msg[0]}")

    if reply is None:
        raise ConnectionError("no reply received from server")
    return reply

def _process_status(reply, print_infos=False, assert_errors=False):
    """Emit warnings/errors from a reply's status map."""
    return_status = reply[5]

    if print_infos and 'infos' in return_status:
        print("Server info:")
        for k in return_status['infos']:
            print(k)

    if 'warnings' in return_status:
        for k in return_status['warnings']:
            warnings.warn(k, MarServerWarning)

    if 'errors' in return_status:
        if assert_errors:
            assert 'errors' not in return_status, return_status['errors'][0]
        else:
            for k in return_status['errors']:
                warnings.warn("SERVER ERROR: " + k, RuntimeWarning)

    return return_status

def command(server_dict: dict, socket: _Socket, print_infos: bool = False, assert_errors: bool = False, **params: Any) -> tuple[list, dict]:
    packet = construct_packet(server_dict, params=params if params else None)
    reply = send_packet(packet, socket)
    return_status = _process_status(reply, print_infos, assert_errors)
    return reply, return_status

def streamed_command(
    server_dict: dict, socket: _Socket, print_infos: bool = False, assert_errors: bool = False, **params: Any,
) -> Iterator[list | tuple[list, dict]]:
    """Send a command expecting intermediate stream messages before the final reply.
    Yields each intermediate message as it arrives. The final (reply, status)
    is the generator's return value (accessible via StopIteration.value)."""
    packet = construct_packet(server_dict, params=params if params else None)
    socket.sendall(msgpack.packb(packet))

    reply = None
    for msg in receive_response(socket):
        if _is_reply(msg):
            reply = msg
        else:
            yield msg

    return_status = _process_status(reply, print_infos, assert_errors)
    yield reply, return_status

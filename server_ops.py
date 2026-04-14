"""Strongly-typed wrappers for MaRCoS server operations.

Each function corresponds to a command understood by ``hardware::run_request``
on the server side and returns a typed result extracted from the reply.

All wrappers accept the common *print_infos* / *assert_errors* flags from
:func:`server_comms.command` and forward them unchanged.  Any extra
``**params`` are forwarded as msgpack request parameters (e.g.
``stream_response=True``).

Backwards-compatible: callers that prefer the raw ``(Reply, StatusDict)``
tuple can keep using :func:`server_comms.command` directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from socket import socket as _Socket
from typing import Any, Literal, NamedTuple, TypedDict

from server_comms import (
    CommandResult,
    Reply,
    StatusDict,
    command,
    close_server_pkt,
    construct_packet,
    send_packet,
    streamed_command,
)

__all__ = [
    # Result types
    "BusTimings",
    "NetThroughput",
    "RegStatus",
    "RxData",
    "ServerKind",
    # Operations
    "acq_rlim",
    "are_you_real",
    "close_server",
    "ctrl",
    "direct",
    "fpga_clk",
    "halt_and_reset",
    "mar_mem",
    "read_mem",
    "read_rx",
    "regrd",
    "regstatus",
    "run_seq",
    "run_seq_streamed",
    "server_version",
    "test_bus",
    "test_net",
]

# ── Shared result types ──────────────────────────────────────────────

class RxData(TypedDict, total=False):
    """RX sample arrays returned by ``read_rx`` and ``run_seq``."""
    rx0_i: list[int]
    rx0_q: list[int]
    rx1_i: list[int]
    rx1_q: list[int]


class RegStatus(NamedTuple):
    """Register snapshot returned by ``regstatus``."""
    exec: int
    status: int
    status_latch: int
    buf_err: int
    buf_full: int
    buf_empty: int
    rx_locs: int


class BusTimings(NamedTuple):
    """Microsecond timings returned by ``test_bus``."""
    null_us: int
    read_us: int
    write_us: int


class NetThroughput(TypedDict):
    """Arrays returned by ``test_net``."""
    array1: list[float]
    array2: list[float]


# ── Helpers ──────────────────────────────────────────────────────────

def _cmd(
    key: str, value: Any, socket: _Socket,
    print_infos: bool = False, assert_errors: bool = False,
    **params: Any,
) -> CommandResult:
    return command({key: value}, socket, print_infos, assert_errors, **params)


# ── Operations ───────────────────────────────────────────────────────

def halt_and_reset(
    socket: _Socket, *, print_infos: bool = False, assert_errors: bool = False,
) -> tuple[bool, StatusDict]:
    """Halt and reset the FSM.  Returns ``True`` if the FSM has halted."""
    reply, status = _cmd("halt_and_reset", 0, socket, print_infos, assert_errors)
    return reply.data["halt_and_reset"], status


def read_mem(
    socket: _Socket, *, print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Read directly from memory (server TODO).  Returns ``"ok"``."""
    reply, status = _cmd("read_mem", 0, socket, print_infos, assert_errors)
    return reply.data["read_mem"], status


def fpga_clk(
    words: tuple[int, int, int], socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Configure the FPGA clock.  *words* is a 3-element tuple of uint32 values.
    Returns ``"ok"`` or ``"err"``."""
    reply, status = _cmd("fpga_clk", list(words), socket, print_infos, assert_errors)
    return reply.data["fpga_clk"], status


def ctrl(
    value: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Write to the main control register.  Returns ``"ok"``."""
    reply, status = _cmd("ctrl", value, socket, print_infos, assert_errors)
    return reply.data["ctrl"], status


def direct(
    value: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Write directly to a buffer.  Returns ``"ok"``."""
    reply, status = _cmd("direct", value, socket, print_infos, assert_errors)
    return reply.data["direct"], status


def regrd(
    index: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[int, StatusDict]:
    """Read one hardware register by *index*.  Returns the register value."""
    reply, status = _cmd("regrd", index, socket, print_infos, assert_errors)
    return reply.data["regrd"], status


def regstatus(
    socket: _Socket, *, print_infos: bool = False, assert_errors: bool = False,
) -> tuple[RegStatus, StatusDict]:
    """Read all status registers.  Returns a :class:`RegStatus` named tuple."""
    reply, status = _cmd("regstatus", 0, socket, print_infos, assert_errors)
    return RegStatus(*reply.data["regstatus"]), status


def mar_mem(
    data: bytes, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Write execution memory.  Returns ``"ok"`` or ``"err"``."""
    reply, status = _cmd("mar_mem", data, socket, print_infos, assert_errors)
    return reply.data["mar_mem"], status


def acq_rlim(
    limit: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[str, StatusDict]:
    """Configure the acquisition retry limit (must be in [1000, 10_000_000]).
    Returns ``"ok"`` or ``"err"``."""
    reply, status = _cmd("acq_rlim", limit, socket, print_infos, assert_errors)
    return reply.data["acq_rlim"], status


def read_rx(
    socket: _Socket, *, print_infos: bool = False, assert_errors: bool = False,
) -> tuple[RxData | str, StatusDict]:
    """Read outstanding RX FIFO data.  Returns an :class:`RxData` dict, or
    ``"ok"`` if there was no data."""
    reply, status = _cmd("read_rx", 0, socket, print_infos, assert_errors)
    return reply.data["read_rx"], status


def run_seq(
    bytecode: bytes, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[RxData | str, StatusDict]:
    """Run a compiled sequence.  Returns an :class:`RxData` dict, or ``"ok"``
    if no RX data was received."""
    reply, status = _cmd("run_seq", bytecode, socket, print_infos, assert_errors)
    return reply.data["run_seq"], status


def run_seq_streamed(
    bytecode: bytes, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> Iterator[RxData | tuple[RxData | str, StatusDict]]:
    """Run a sequence with RX streaming.

    Yields intermediate :class:`RxData` chunks.  The last yielded value is a
    ``(RxData | str, StatusDict)`` tuple with the final reply."""
    for msg in streamed_command(
        {"run_seq": bytecode}, socket,
        print_infos=print_infos, assert_errors=assert_errors,
        stream_response=True,
    ):
        if isinstance(msg, tuple):
            reply, status = msg
            yield reply.data["run_seq"], status
        else:
            # intermediate chunk: [type, index, {rx0_i: [...], ...}]
            yield msg[2]


def test_net(
    data_size: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[NetThroughput, StatusDict]:
    """Test client-server network throughput with *data_size* elements."""
    reply, status = _cmd("test_net", data_size, socket, print_infos, assert_errors)
    return reply.data["test_net"], status


def test_bus(
    n_tests: int, socket: _Socket, *,
    print_infos: bool = False, assert_errors: bool = False,
) -> tuple[BusTimings, StatusDict]:
    """Test bus read/write throughput.  Returns microsecond timings."""
    reply, status = _cmd("test_bus", n_tests, socket, print_infos, assert_errors)
    return BusTimings(*reply.data["test_bus"]), status


ServerKind = Literal["hardware", "simulation", "software"]


def are_you_real(
    socket: _Socket, *, print_infos: bool = False, assert_errors: bool = False,
) -> tuple[ServerKind, StatusDict]:
    """Check whether the server runs on hardware, simulation, or software."""
    reply, status = _cmd("are_you_real", 0, socket, print_infos, assert_errors)
    return reply.data["are_you_real"], status


def close_server(socket: _Socket) -> Reply:
    """Send the close-server packet.  The server will shut down."""
    return send_packet(construct_packet({}, 0, command=close_server_pkt), socket)


def server_version(socket: _Socket) -> int:
    """Return the server's protocol version uint.

    Sends a cheap ``are_you_real`` probe and extracts the version from the
    protocol frame (``Reply.version``)."""
    reply, _status = _cmd("are_you_real", 0, socket)
    return reply.version

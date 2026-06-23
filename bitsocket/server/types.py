"""Shared small types for bitsocket.server."""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class Handshake:
    """Request metadata captured when the underlying WebSocket connection
    was first established (mirrors the `handshake` object attached to every
    ServerSocket in bit-socket-node)."""

    headers: Any  # websockets.datastructures.Headers (dict-like, case-insensitive)
    path: str
    time: float


class Emitter:
    """A tiny fluent helper returned by to()/broadcast so calls can be
    chained like ``await socket.to("room").emit("event", payload)``."""

    def __init__(self, emit_fn: Callable[[str, Any], Awaitable[None]]):
        self._emit_fn = emit_fn

    async def emit(self, event: str, payload: Any = None) -> None:
        await self._emit_fn(event, payload)

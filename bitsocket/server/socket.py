"""bitsocket.server.socket - Socket, mirroring src/server/socket.js."""

import asyncio
import inspect
import secrets
from typing import Any, Callable, Dict, List, Optional

import websockets

from ..protocol import (
    FRAME_ACK,
    FRAME_EVENT,
    FRAME_JOIN,
    FRAME_LEAVE,
    FRAME_PING,
    FRAME_PONG,
    Schema,
    encode_frame,
)
from .types import Emitter, Handshake


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


def _generate_socket_id() -> str:
    return secrets.token_hex(12)


class Socket:
    """One client multiplexed onto one namespace of one physical WebSocket
    connection (mirrors ServerSocket in src/server/socket.js). A single
    physical connection has one Socket per namespace it has joined."""

    def __init__(self, websocket, server: "Any", nsp: str, handshake: Handshake, hub: "Any"):
        self.id = _generate_socket_id()
        self.nsp = nsp
        self.handshake = handshake
        self.server = server
        self.websocket = websocket
        self.hub = hub
        self.rooms: set = {self.id}  # auto-join own id room
        self.listeners: Dict[str, Callable] = {}
        self.middlewares: List[Callable] = []

    @property
    def namespace(self):
        return self.server.namespaces.get(self.nsp)

    async def send(self, buf: bytes) -> None:
        await self.hub.send(buf)

    async def close_conn(self) -> None:
        """Closes the entire underlying physical WebSocket connection,
        affecting every namespace multiplexed onto it (matches
        bit-socket-node, where a namespace middleware rejection closes the
        shared websocket rather than just detaching one namespace)."""
        try:
            await self.websocket.close()
        except Exception:
            pass

    def use(self, fn: Callable) -> "Socket":
        """Register event-level middleware: async def fn(packet, next)
        where packet is a mutable [event, payload] list."""
        self.middlewares.append(fn)
        return self

    def on(self, event_or_schema, callback: Callable) -> "Socket":
        event = event_or_schema.name if isinstance(event_or_schema, Schema) else event_or_schema
        self.listeners[event] = callback
        return self

    async def emit(self, event_or_schema, payload: Any = None) -> None:
        event = event_or_schema.name if isinstance(event_or_schema, Schema) else event_or_schema
        ns = self.namespace
        serializers = self.server.serializers
        if ns is not None and event in ns.schemas:
            from ..protocol import Serializers

            schema = ns.schemas[event]
            serializers = Serializers(encode_payload=schema.encode_payload)
        buf = encode_frame(FRAME_EVENT, nsp=self.nsp, event=event, payload=payload, serializers=serializers)
        await self.send(buf)

    @property
    def broadcast(self) -> Emitter:
        ns = self.namespace

        async def _emit(event: str, payload: Any):
            if ns is not None:
                await ns._emit_excluding(event, payload, exclude=self)

        return Emitter(_emit)

    def to(self, room: str) -> Emitter:
        ns = self.namespace

        async def _emit(event: str, payload: Any):
            if ns is not None:
                await ns._to_excluding(room, event, payload, exclude=self)

        return Emitter(_emit)

    def join(self, room: str) -> None:
        self.rooms.add(room)

    def leave(self, room: str) -> None:
        self.rooms.discard(room)

    def has_room(self, room: str) -> bool:
        return room in self.rooms

    async def dispatch(self, frame) -> None:
        """Routes one already-decoded frame addressed to this socket's
        namespace (mirrors the switch in ServerSocket.initTransport's
        ws.on('message', ...) handler)."""
        if frame.nsp != self.nsp:
            return

        if frame.type == FRAME_EVENT:
            await self._process_event(frame)
        elif frame.type == FRAME_JOIN:
            room = (frame.payload or {}).get("room") if isinstance(frame.payload, dict) else None
            if room:
                self.join(room)
        elif frame.type == FRAME_LEAVE:
            room = (frame.payload or {}).get("room") if isinstance(frame.payload, dict) else None
            if room:
                self.leave(room)
        elif frame.type == FRAME_PING:
            buf = encode_frame(FRAME_PONG, nsp=self.nsp, serializers=self.server.serializers)
            await self.send(buf)

    async def _process_event(self, frame) -> None:
        packet = [frame.event, frame.payload]

        for mw in list(self.middlewares):
            box: Dict[str, Any] = {}

            def next_fn(err: Optional[Exception] = None, _box=box):
                _box["called"] = True
                _box["err"] = err

            try:
                result = mw(packet, next_fn)
                await _maybe_await(result)
            except Exception:
                return  # unhandled exception halts the pipeline

            if not box.get("called"):
                return  # never called next(): silently stall, matching JS

            if box.get("err"):
                if frame.ack_id > 0:
                    err = box["err"]
                    buf = encode_frame(
                        FRAME_ACK,
                        nsp=self.nsp,
                        event="error",
                        ack_id=frame.ack_id,
                        payload={"message": str(err)},
                        serializers=self.server.serializers,
                    )
                    await self.send(buf)
                return

        final_event, final_payload = packet[0], packet[1]
        handler = self.listeners.get(final_event)
        if not handler:
            return

        ack = None
        if frame.ack_id > 0:
            ack_id = frame.ack_id

            async def ack(response_payload: Any = None, _event=final_event, _ack_id=ack_id):
                buf = encode_frame(
                    FRAME_ACK,
                    nsp=self.nsp,
                    event=_event,
                    ack_id=_ack_id,
                    payload=response_payload,
                    serializers=self.server.serializers,
                )
                await self.send(buf)

        await _maybe_await(handler(final_payload, ack))

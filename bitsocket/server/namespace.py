"""bitsocket.server.namespace - Namespace, mirroring src/server/namespace.js."""

import asyncio
import inspect
from typing import Any, Callable, Dict, List, Optional

from ..protocol import FRAME_ACK, FRAME_EVENT, Schema, Serializers, encode_frame
from .types import Emitter


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


class Namespace:
    """Groups sockets, middleware, schemas, and rooms under a single path.
    The root namespace is always "/"."""

    def __init__(self, name: str, server: "Any"):
        self.name = name
        self.server = server
        self.sockets: set = set()
        self.middlewares: List[Callable] = []
        self.connection_handlers: List[Callable] = []
        self.schemas: Dict[str, Schema] = {}

    def schema(self, schemas) -> "Namespace":
        """Register one or more schemas, keyed by their schema name.
        Accepts a single Schema or an iterable of Schemas."""
        if isinstance(schemas, (list, tuple)):
            for s in schemas:
                self.schemas[s.name] = s
        else:
            self.schemas[schemas.name] = schemas
        return self

    def export_schemas(self) -> Dict[str, Any]:
        """Returns {event_name: definition} for embedding in a CONNECT
        frame payload, enabling client auto-sync."""
        return {event: schema.definition for event, schema in self.schemas.items()}

    def use(self, fn: Callable) -> "Namespace":
        """Register connection middleware: async def fn(socket, next)."""
        self.middlewares.append(fn)
        return self

    def on_connection(self, handler: Callable) -> "Namespace":
        """Register a handler invoked for every socket that successfully
        joins this namespace (after all connection middleware passes)."""
        self.connection_handlers.append(handler)
        return self

    async def fire_connection(self, sock) -> None:
        for handler in list(self.connection_handlers):
            await _maybe_await(handler(sock))

    def add_socket(self, sock) -> None:
        self.sockets.add(sock)

    def remove_socket(self, sock) -> None:
        self.sockets.discard(sock)

    def _serializers_for(self, event: str) -> Serializers:
        schema = self.schemas.get(event)
        if schema:
            return Serializers(encode_payload=schema.encode_payload)
        return self.server.serializers

    async def emit(self, event: str, payload: Any = None, exclude=None) -> None:
        await self._emit_excluding(event, payload, exclude)

    async def _emit_excluding(self, event: str, payload: Any, exclude=None) -> None:
        ser = self._serializers_for(event)
        buf = encode_frame(FRAME_EVENT, nsp=self.name, event=event, payload=payload, serializers=ser)
        for sock in list(self.sockets):
            if sock is exclude:
                continue
            await sock.send(buf)

    def to(self, room: str, exclude=None) -> Emitter:
        async def _emit(event: str, payload: Any):
            await self._to_excluding(room, event, payload, exclude)

        return Emitter(_emit)

    async def _to_excluding(self, room: str, event: str, payload: Any, exclude=None) -> None:
        ser = self._serializers_for(event)
        buf = encode_frame(FRAME_EVENT, nsp=self.name, event=event, payload=payload, serializers=ser)
        for sock in list(self.sockets):
            if sock is exclude:
                continue
            if sock.has_room(room):
                await sock.send(buf)

    async def run_middlewares(self, sock) -> bool:
        """Walks the connection middleware chain in order. Returns True if
        every middleware called next() with no error. On the first
        rejection (or a middleware that never calls next), sends an "error"
        ack frame (if rejected with an error) and closes the connection,
        then returns False."""
        for mw in list(self.middlewares):
            box: Dict[str, Any] = {}

            def next_fn(err: Optional[Exception] = None, _box=box):
                _box["called"] = True
                _box["err"] = err

            result = mw(sock, next_fn)
            await _maybe_await(result)

            if not box.get("called"):
                return False  # middleware never called next(): silently stall, matching JS

            if box.get("err"):
                err = box["err"]
                buf = encode_frame(
                    FRAME_ACK,
                    nsp=self.name,
                    event="error",
                    payload={"message": str(err)},
                    serializers=self.server.serializers,
                )
                await sock.send(buf)
                await sock.close_conn()
                return False
        return True

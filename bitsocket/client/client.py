"""bitsocket.client.client - BitSocketClient, mirroring src/client/client.js.

Adaptation note: a Python ``__init__`` can't be ``async``, so unlike the
JS constructor (which kicks off the WebSocket dial asynchronously and
returns immediately), this client does *not* connect itself. Construct it,
register your ``on(...)`` handlers, then ``await client.connect()``::

    client = BitSocketClient(url, nsp="/secure-gateway", headers={...})
    client.on("connect", on_connect)
    await client.connect()

Everything after that - reconnect scheduling, heartbeat, namespace
multiplexing, ack callbacks, schema auto-sync - mirrors the original.
"""

import asyncio
import inspect
import logging
import random
from typing import Any, Callable, Dict, Optional

from websockets.asyncio.client import connect as ws_connect

from ..protocol import (
    FRAME_ACK,
    FRAME_CONNECT,
    FRAME_EVENT,
    FRAME_JOIN,
    FRAME_LEAVE,
    FRAME_PING,
    FRAME_PONG,
    Schema,
    Serializers,
    decode_frame_header,
    default_serializers,
    encode_frame,
)
from .namespace import ClientNamespace

logger = logging.getLogger("bitsocket.client")


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


def _schema_key(nsp: str) -> str:
    return "root" if nsp == "/" else nsp.lstrip("/")


class BitSocketClient:
    def __init__(
        self,
        url: str,
        nsp: str = "/",
        serializers: Optional[Serializers] = None,
        use_schemas: bool = True,
        auto_reconnect: bool = True,
        max_attempts: int = 15,
        base_delay: float = 1.0,
        max_delay: float = 7.0,
        ping_interval: float = 20.0,
        pong_timeout: float = 8.0,
        headers: Optional[dict] = None,
        **connect_kwargs: Any,
    ):
        self.url = url
        self.serializers = serializers or default_serializers()
        self.use_schemas = use_schemas
        self.auto_reconnect = auto_reconnect
        self.reconnect_attempts = 0
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.headers = headers
        self._connect_kwargs = connect_kwargs

        self.ack_callbacks: Dict[int, Callable] = {}
        self.ack_counter = 1

        # nspKey -> {eventName: Schema}; mirrors `this.schemas` in the JS
        # client. Auto-synced schemas are ALSO flattened into _flat_schemas
        # ("so user can access directly via root.schemas.EVENT_NAME").
        self.schemas: Dict[str, Dict[str, Schema]] = {}
        self._flat_schemas: Dict[str, Schema] = {}

        self.namespaces: Dict[str, ClientNamespace] = {}

        self.nsp = nsp or "/"
        self.root_namespace = ClientNamespace(self, self.nsp)
        self.namespaces[self.nsp] = self.root_namespace

        self.ws = None
        self._closed_by_user = False
        self._read_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._pong_timer_task: Optional[asyncio.Task] = None

    # --- root namespace convenience delegation ----------------------------------

    def on(self, event_or_schema, callback: Callable) -> "BitSocketClient":
        self.root_namespace.on(event_or_schema, callback)
        return self

    async def emit(self, event_or_schema, payload: Any = None, callback: Optional[Callable] = None) -> None:
        await self.root_namespace.emit(event_or_schema, payload, callback)

    async def join(self, room: str) -> None:
        await self.root_namespace.join(room)

    async def leave(self, room: str) -> None:
        await self.root_namespace.leave(room)

    def schema(self, schema_obj_or_list) -> "BitSocketClient":
        self.root_namespace.schema(schema_obj_or_list)
        return self

    def use(self, fn: Callable) -> "BitSocketClient":
        self.root_namespace.use(fn)
        return self

    # --- schema introspection ----------------------------------------------------

    def get_schema(self, event_name: str) -> Optional[Schema]:
        """Returns a server-auto-synced schema by event name, flattened
        across whichever namespace it was declared on (mirrors reading
        `client.schemas.EVENT_NAME` directly in the JS implementation)."""
        return self._flat_schemas.get(event_name)

    def schemas_for(self, nsp: str) -> Dict[str, Schema]:
        return dict(self.schemas.get(_schema_key(nsp), {}))

    # --- namespaces ---------------------------------------------------------------

    def of(self, nsp: str) -> ClientNamespace:
        is_new = nsp not in self.namespaces
        if is_new:
            self.namespaces[nsp] = ClientNamespace(self, nsp)
        ns = self.namespaces[nsp]
        if is_new and self.ws is not None and nsp != "/":
            asyncio.ensure_future(self._send_connect_frame(nsp))
        return ns

    def _get_namespace(self, nsp: str) -> Optional[ClientNamespace]:
        return self.namespaces.get(nsp)

    # --- transport -----------------------------------------------------------------

    async def _safe_send(self, buf: bytes) -> bool:
        if self.ws is None:
            return False
        try:
            await self.ws.send(buf)
            return True
        except Exception:
            return False

    async def _send_connect_frame(self, nsp: str) -> None:
        buf = encode_frame(FRAME_CONNECT, nsp=nsp, serializers=self.serializers)
        await self._safe_send(buf)

    async def connect(self) -> None:
        """Dials the WebSocket and starts reading. Mirrors connect() in the
        JS client (including being safe to call again for reconnects)."""
        try:
            ws = await ws_connect(self.url, additional_headers=self.headers, **self._connect_kwargs)
        except Exception:
            await self._handle_close()
            return

        self.ws = ws
        self.reconnect_attempts = 0

        # Re-join every non-root namespace already in use (mirrors ws.onopen).
        for nsp in list(self.namespaces.keys()):
            if nsp != "/":
                await self._send_connect_frame(nsp)

        self._read_task = asyncio.ensure_future(self._read_loop(ws))

    async def _read_loop(self, ws) -> None:
        try:
            async for message in ws:
                await self._handle_message(message)
        except Exception:
            pass
        finally:
            await self._handle_close()

    async def _handle_message(self, raw: bytes) -> None:
        try:
            header, payload_bytes = decode_frame_header(raw)
        except Exception as exc:
            logger.error("[BitSocket Client] Error parsing incoming transmission package frame: %s", exc)
            return

        target_ns = self._get_namespace(header.nsp)
        if target_ns is None:
            return  # Drop frame if no multiplexed client namespace exists for it

        if header.type == FRAME_CONNECT:
            if self.use_schemas and payload_bytes:
                self._absorb_schema_payload(header.nsp, payload_bytes)
            await target_ns._trigger_event("connect", None)
            if header.nsp == self.nsp:
                self._setup_heartbeat()

        elif header.type == FRAME_EVENT:
            try:
                payload = self._decode_app_payload(payload_bytes, header.event, header.nsp)
            except Exception as exc:
                logger.error("[BitSocket Client] Error parsing incoming transmission package frame: %s", exc)
                return
            await target_ns._trigger_event(header.event, payload)

        elif header.type == FRAME_ACK:
            try:
                payload = self._decode_app_payload(payload_bytes, header.event, header.nsp)
            except Exception as exc:
                logger.error("[BitSocket Client] Error parsing incoming transmission package frame: %s", exc)
                return
            cb = self.ack_callbacks.pop(header.ack_id, None)
            if cb:
                await _maybe_await(cb(payload))

        elif header.type == FRAME_PONG:
            self._clear_pong_timeout()

    def _decode_app_payload(self, payload_bytes: bytes, event: str, nsp: str) -> Any:
        if not payload_bytes:
            return None
        schema = self.schemas.get(_schema_key(nsp), {}).get(event)
        if schema:
            return schema.decode_payload(payload_bytes)
        return self.serializers.decode_payload(payload_bytes)

    def _absorb_schema_payload(self, nsp: str, payload_bytes: bytes) -> None:
        """Reconstructs Schemas from a CONNECT frame's payload. Unlike the
        Go port, no special order-preserving decoder is needed here: Python
        dicts (3.7+) preserve insertion order, so a plain
        `self.serializers.decode_payload(...)` call already yields
        definitions with their fields in the order the server declared
        them - the same guarantee the original JS implementation relies on
        from V8's object property ordering."""
        try:
            raw = self.serializers.decode_payload(payload_bytes)
        except Exception:
            return
        if not raw:
            return

        if nsp == "/":
            for nsp_key, defs in raw.items():
                bucket = self.schemas.setdefault(nsp_key, {})
                for event_name, definition in (defs or {}).items():
                    try:
                        schema = Schema(event_name, definition)
                    except Exception:
                        continue
                    bucket[event_name] = schema
                    self._flat_schemas[event_name] = schema  # flatten
        else:
            nsp_key = _schema_key(nsp)
            bucket = self.schemas.setdefault(nsp_key, {})
            for event_name, definition in raw.items():
                try:
                    schema = Schema(event_name, definition)
                except Exception:
                    continue
                bucket[event_name] = schema
                self._flat_schemas[event_name] = schema

    # --- heartbeat -------------------------------------------------------------------

    def _setup_heartbeat(self) -> None:
        self._cancel_heartbeat()

        async def _loop():
            try:
                while True:
                    await asyncio.sleep(self.ping_interval)
                    if self.ws is None:
                        continue
                    buf = encode_frame(FRAME_PING, nsp="/", serializers=self.serializers)
                    if await self._safe_send(buf):
                        self._arm_pong_timeout()
            except asyncio.CancelledError:
                pass

        self._heartbeat_task = asyncio.ensure_future(_loop())

    def _arm_pong_timeout(self) -> None:
        self._clear_pong_timeout()

        async def _timeout():
            try:
                await asyncio.sleep(self.pong_timeout)
                logger.warning(
                    "[BitSocket Client] Ping-Pong Heartbeat Timeout detected. "
                    "Restructuring channel link connection..."
                )
                if self.ws is not None:
                    await self.ws.close()
            except asyncio.CancelledError:
                pass

        self._pong_timer_task = asyncio.ensure_future(_timeout())

    def _clear_pong_timeout(self) -> None:
        if self._pong_timer_task is not None:
            self._pong_timer_task.cancel()
            self._pong_timer_task = None

    def _cancel_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        self._clear_pong_timeout()

    # --- close / reconnect -------------------------------------------------------------

    async def _handle_close(self) -> None:
        self.ws = None
        self._cancel_heartbeat()

        for ns in list(self.namespaces.values()):
            await ns._trigger_event("disconnect", None)

        if not self._closed_by_user and self.auto_reconnect and self.reconnect_attempts < self.max_attempts:
            self._execute_reconnection_schedule()

    def _execute_reconnection_schedule(self) -> None:
        self.reconnect_attempts += 1
        attempt = self.reconnect_attempts
        delay = min(self.base_delay * (1.5**attempt) + random.random() * 0.3, self.max_delay)

        async def _after_delay():
            await asyncio.sleep(delay)
            for ns in list(self.namespaces.values()):
                await ns._trigger_event("reconnecting", attempt)
            await self.connect()

        asyncio.ensure_future(_after_delay())

    # --- outgoing frames, called by ClientNamespace ---------------------------------------

    async def _do_emit(self, event_or_schema, payload: Any, callback: Optional[Callable], target_nsp: str) -> None:
        event = event_or_schema.name if isinstance(event_or_schema, Schema) else event_or_schema

        ack_id = 0
        if callback:
            ack_id = self.ack_counter
            self.ack_counter += 1
            self.ack_callbacks[ack_id] = callback

        nsp_key = _schema_key(target_nsp)
        serializers = self.serializers
        schema = self.schemas.get(nsp_key, {}).get(event)
        if schema:
            serializers = Serializers(encode_payload=schema.encode_payload)

        buf = encode_frame(FRAME_EVENT, nsp=target_nsp, event=event, ack_id=ack_id, payload=payload, serializers=serializers)

        if self.ws is not None:
            await self._safe_send(buf)
        else:
            logger.error(
                "[BitSocket Client] Failed data delivery initialization: Pipeline currently in "
                "closed state window. [Event: %s]",
                event,
            )

    async def _do_join(self, room: str, nsp: str) -> None:
        buf = encode_frame(FRAME_JOIN, nsp=nsp, payload={"room": room}, serializers=self.serializers)
        if self.ws is not None:
            await self._safe_send(buf)

    async def _do_leave(self, room: str, nsp: str) -> None:
        buf = encode_frame(FRAME_LEAVE, nsp=nsp, payload={"room": room}, serializers=self.serializers)
        if self.ws is not None:
            await self._safe_send(buf)

    # --- shutdown ---------------------------------------------------------------------------

    async def close(self) -> None:
        """Stops auto-reconnection and closes the underlying WebSocket
        connection."""
        self._closed_by_user = True
        self._cancel_heartbeat()
        if self.ws is not None:
            await self.ws.close()

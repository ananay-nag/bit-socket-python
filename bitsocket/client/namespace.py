"""bitsocket.client.namespace - ClientNamespace, mirroring ClientNamespace in
src/client/client.js."""

import inspect
from typing import Any, Callable, Dict, List, Optional

from ..protocol import Schema


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


class ClientNamespace:
    """One multiplexed channel of a Client, bound to a single server-side
    namespace path."""

    def __init__(self, client: "Any", nsp: str):
        self.client = client
        self.nsp = nsp
        self.listeners: Dict[str, Callable] = {}
        self.middlewares: List[Callable] = []

        nsp_key = "root" if nsp == "/" else nsp.lstrip("/")
        self.client.schemas.setdefault(nsp_key, {})

    def _nsp_key(self) -> str:
        return "root" if self.nsp == "/" else self.nsp.lstrip("/")

    def schema(self, schema_obj_or_list) -> "ClientNamespace":
        nsp_key = self._nsp_key()
        bucket = self.client.schemas.setdefault(nsp_key, {})
        if isinstance(schema_obj_or_list, (list, tuple)):
            for s in schema_obj_or_list:
                bucket[s.name] = s
        else:
            bucket[schema_obj_or_list.name] = schema_obj_or_list
        return self

    def on(self, event_or_schema, callback: Callable) -> "ClientNamespace":
        event = event_or_schema.name if isinstance(event_or_schema, Schema) else event_or_schema
        self.listeners[event] = callback
        return self

    async def emit(self, event_or_schema, payload: Any = None, callback: Optional[Callable] = None) -> None:
        await self.client._do_emit(event_or_schema, payload, callback, self.nsp)

    async def join(self, room: str) -> None:
        await self.client._do_join(room, self.nsp)

    async def leave(self, room: str) -> None:
        await self.client._do_leave(room, self.nsp)

    def use(self, fn: Callable) -> "ClientNamespace":
        self.middlewares.append(fn)
        return self

    async def close(self) -> None:
        self.listeners = {}
        if self.nsp == "/":
            await self.client.close()

    async def _trigger_event(self, event: str, data: Any) -> None:
        if self.middlewares:
            packet = [event, data]
            for mw in list(self.middlewares):
                box: Dict[str, Any] = {}

                def next_fn(err=None, _box=box):
                    _box["called"] = True
                    _box["err"] = err

                try:
                    result = mw(packet, next_fn)
                    await _maybe_await(result)
                except Exception:
                    return  # unhandled exception halts the pipeline

                if not box.get("called") or box.get("err"):
                    return  # halt on error / never-called-next

            handler = self.listeners.get(packet[0])
            if handler:
                await _maybe_await(handler(packet[1]))
        else:
            handler = self.listeners.get(event)
            if handler:
                await _maybe_await(handler(data))

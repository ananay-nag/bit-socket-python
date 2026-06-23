"""bitsocket.server.server - BitSocketServer, mirroring src/server/server.js.

Built on the `websockets` library's asyncio server. A BitSocketServer
upgrades every incoming connection regardless of URL path (matching
bit-socket-node, which doesn't restrict upgrades to a specific path) and
multiplexes namespaces over each connection.
"""

import asyncio
import http
import logging
import time
from typing import Any, Callable, Dict, Optional

from websockets.asyncio.server import serve

from ..protocol import FRAME_CONNECT, Serializers, default_serializers, encode_frame
from .hub import ConnHub
from .namespace import Namespace
from .socket import Socket
from .types import Emitter, Handshake

logger = logging.getLogger("bitsocket.server")


class BitSocketServer:
    def __init__(
        self,
        port: Optional[int] = None,
        host: str = "0.0.0.0",
        serializers: Optional[Serializers] = None,
        use_schemas: bool = True,
        cors_origins: Optional[list] = None,
        **serve_kwargs: Any,
    ):
        self.port = port
        self.host = host
        self.serializers = serializers or default_serializers()
        self.use_schemas = use_schemas
        self.cors_origins = cors_origins or ["*"]
        self._serve_kwargs = serve_kwargs

        self.namespaces: Dict[str, Namespace] = {}
        self._server = None

        self.of("/")

    # --- namespace management -------------------------------------------------

    def of(self, name: str) -> Namespace:
        if name not in self.namespaces:
            self.namespaces[name] = Namespace(name, self)
        return self.namespaces[name]

    # --- root-namespace convenience delegation ---------------------------------

    def use(self, fn: Callable) -> "BitSocketServer":
        self.of("/").use(fn)
        return self

    def on_connection(self, handler: Callable) -> "BitSocketServer":
        self.of("/").on_connection(handler)
        return self

    async def emit(self, event: str, payload: Any = None) -> None:
        await self.of("/").emit(event, payload)

    def to(self, room: str, nsp: str = "/") -> Emitter:
        ns = self.namespaces.get(nsp)
        if ns is None:
            async def _noop(event, payload):
                return None

            return Emitter(_noop)
        return ns.to(room)

    # --- schema introspection ---------------------------------------------------

    @staticmethod
    def _schema_export_key(nsp: str) -> str:
        return "root" if nsp == "/" else nsp.lstrip("/")

    def schemas(self) -> Dict[str, Dict[str, Any]]:
        return {self._schema_export_key(nsp): dict(ns.schemas) for nsp, ns in self.namespaces.items()}

    def export_all_schemas(self) -> Dict[str, Any]:
        return {self._schema_export_key(nsp): ns.export_schemas() for nsp, ns in self.namespaces.items()}

    # --- CORS / connection handling ---------------------------------------------

    def _origin_allowed(self, origin: Optional[str]) -> bool:
        if "*" in self.cors_origins:
            return True
        return origin in self.cors_origins

    async def _process_request(self, connection, request):
        origin = request.headers.get("Origin")
        if not self._origin_allowed(origin):
            return connection.respond(http.HTTPStatus.FORBIDDEN, "origin not allowed\n")
        return None

    async def _handle_connection(self, websocket) -> None:
        request = websocket.request
        handshake = Handshake(headers=request.headers, path=request.path, time=time.time())

        hub = ConnHub(websocket, self, handshake)
        root_ns = self.of("/")
        root_sock = Socket(websocket, self, "/", handshake, hub)

        passed = await root_ns.run_middlewares(root_sock)
        if passed:
            root_ns.add_socket(root_sock)
            hub.sockets["/"] = root_sock

            connect_payload = self.export_all_schemas() if self.use_schemas else None
            buf = encode_frame(FRAME_CONNECT, nsp="/", payload=connect_payload, serializers=self.serializers)
            await root_sock.send(buf)

            await root_ns.fire_connection(root_sock)

        try:
            async for message in websocket:
                await hub.handle_message(message)
        except Exception:
            pass
        finally:
            await hub.handle_close()

    # --- lifecycle ---------------------------------------------------------------

    async def start(self):
        """Starts listening and returns the underlying websockets Server."""
        if self.port is None:
            raise ValueError("BitSocketServer.start() requires `port` to be set")
        self._server = await serve(
            self._handle_connection,
            self.host,
            self.port,
            process_request=self._process_request,
            **self._serve_kwargs,
        )
        logger.info("[BitSocket Core Server Engine Initialized on Port %s]", self.port)
        return self._server

    async def serve_forever(self) -> None:
        """Starts the server (if not already started) and blocks until it's
        closed. Suitable for `asyncio.run(server.serve_forever())`."""
        if self._server is None:
            await self.start()
        await self._server.wait_closed()

    def run(self) -> None:
        """Blocking convenience helper equivalent to
        `asyncio.run(server.serve_forever())`."""
        asyncio.run(self.serve_forever())

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

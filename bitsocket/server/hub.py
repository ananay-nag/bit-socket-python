"""bitsocket.server.hub - per-connection frame routing across multiplexed
namespaces. One physical WebSocket can carry several namespace Sockets (one
connection, N namespaces); this hub decodes each incoming frame once and
dispatches it to the right Socket, and serializes outgoing writes."""

import asyncio

import websockets

from ..protocol import FRAME_ACK, FRAME_CONNECT, Serializers, decode_frame, encode_frame


class ConnHub:
    def __init__(self, websocket, server, handshake):
        self.websocket = websocket
        self.server = server
        self.handshake = handshake
        self.sockets = {}  # nsp -> Socket
        self._send_lock = asyncio.Lock()

    async def send(self, buf: bytes) -> None:
        async with self._send_lock:
            try:
                await self.websocket.send(buf)
            except websockets.exceptions.ConnectionClosed:
                pass

    async def handle_message(self, raw: bytes) -> None:
        def decode_with_event(buf, event, nsp):
            ns = self.server.namespaces.get(nsp)
            if ns is not None:
                schema = ns.schemas.get(event)
                if schema:
                    return schema.decode_payload(buf)
            return self.server.serializers.decode_payload(buf)

        serializers = Serializers(
            encode_payload=self.server.serializers.encode_payload,
            decode_payload=self.server.serializers.decode_payload,
            decode_payload_with_event=decode_with_event,
        )

        try:
            frame = decode_frame(raw, serializers)
        except Exception:
            return

        if frame.type == FRAME_CONNECT:
            if frame.nsp != "/":
                await self._handle_namespace_connect(frame.nsp)
            return

        sock = self.sockets.get(frame.nsp)
        if not sock:
            return
        await sock.dispatch(frame)

    async def _handle_namespace_connect(self, nsp: str) -> None:
        from .socket import Socket  # local import to avoid a circular import

        ns = self.server.namespaces.get(nsp)
        if ns is None:
            buf = encode_frame(
                FRAME_ACK,
                nsp=nsp,
                event="error",
                payload={"message": f"Requested namespace '{nsp}' does not exist on cluster."},
                serializers=self.server.serializers,
            )
            await self.send(buf)
            return

        sock = Socket(self.websocket, self.server, nsp, self.handshake, self)
        passed = await ns.run_middlewares(sock)
        if not passed:
            return

        ns.add_socket(sock)
        self.sockets[nsp] = sock

        connect_payload = ns.export_schemas() if self.server.use_schemas else None
        buf = encode_frame(FRAME_CONNECT, nsp=nsp, payload=connect_payload, serializers=self.server.serializers)
        await sock.send(buf)

        await ns.fire_connection(sock)

    async def handle_close(self) -> None:
        socks = list(self.sockets.values())
        self.sockets.clear()
        for sock in socks:
            ns = self.server.namespaces.get(sock.nsp)
            if ns is not None:
                ns.remove_socket(sock)

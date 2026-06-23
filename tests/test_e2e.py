import asyncio

import pytest

from bitsocket.client import BitSocketClient
from bitsocket.protocol import BOOLEAN, STRING, UINT32, Schema
from bitsocket.server import BitSocketServer


@pytest.mark.asyncio
async def test_e2e_connect_emit_ack_rooms_schema_sync():
    sync_schema = Schema("SYNC_TEST", {"id": UINT32, "label": STRING})

    io = BitSocketServer(port=18182)
    secure = io.of("/secure-gateway")
    secure.schema(sync_schema)

    async def auth_middleware(sock, next_fn):
        token = sock.handshake.headers.get("X-Auth-Token")
        if token == "enterprise-payload-passkey":
            next_fn(None)
        else:
            next_fn(Exception("ERR_AUTH_FAILURE"))

    secure.use(auth_middleware)

    join_event = asyncio.Event()

    async def on_connection(sock):
        async def on_provision(payload, ack):
            sock.join(payload["room"])
            join_event.set()
            if ack:
                await ack({"status": 200})

        sock.on("cluster:provision", on_provision)

        async def on_sync(payload, ack):
            await sock.to("GRPC").emit("cluster:sync", payload)

        sock.on(sync_schema, on_sync)

    secure.on_connection(on_connection)

    await io.start()
    try:
        headers = {"X-Auth-Token": "enterprise-payload-passkey"}

        c1 = BitSocketClient("ws://127.0.0.1:18182", nsp="/secure-gateway", headers=headers)
        c2 = BitSocketClient("ws://127.0.0.1:18182", nsp="/secure-gateway", headers=headers)

        c1_connected = asyncio.Event()
        c2_connected = asyncio.Event()
        c1.on("connect", lambda payload: c1_connected.set())
        c2.on("connect", lambda payload: c2_connected.set())

        await c1.connect()
        await c2.connect()

        await asyncio.wait_for(c1_connected.wait(), timeout=2)
        await asyncio.wait_for(c2_connected.wait(), timeout=2)

        # schema auto-sync: client should now know SYNC_TEST without manual registration
        for _ in range(200):
            if c1.get_schema("SYNC_TEST") is not None:
                break
            await asyncio.sleep(0.01)
        assert c1.get_schema("SYNC_TEST") is not None

        ack_result = {}
        ack_event = asyncio.Event()

        async def on_ack(payload):
            ack_result["payload"] = payload
            ack_event.set()

        await c2.emit("cluster:provision", {"room": "GRPC"}, on_ack)
        await asyncio.wait_for(ack_event.wait(), timeout=2)
        assert ack_result["payload"]["status"] == 200
        await asyncio.wait_for(join_event.wait(), timeout=2)

        sync_result = {}
        sync_event = asyncio.Event()

        async def on_sync_received(payload):
            sync_result["payload"] = payload
            sync_event.set()

        c2.on("cluster:sync", on_sync_received)

        schema = c1.get_schema("SYNC_TEST")
        await c1.emit(schema, {"id": 77, "label": "node-7"})

        await asyncio.wait_for(sync_event.wait(), timeout=2)
        assert sync_result["payload"]["id"] == 77
        assert sync_result["payload"]["label"] == "node-7"

        await c1.close()
        await c2.close()
    finally:
        await io.close()

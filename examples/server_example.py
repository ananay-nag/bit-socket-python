"""Example BitSocket server with a secured namespace, connection
middleware, rooms, schema-based encoding, and acks. A Python port of
bit-socket-node's test/manual/app.js.

Run with: python3 examples/server_example.py
"""

import asyncio
import logging

from bitsocket.protocol import BOOLEAN, STRING, UINT32, Schema
from bitsocket.server import BitSocketServer

logging.basicConfig(level=logging.INFO)


async def main():
    sync_schema = Schema("SYNC_TEST", {"id": UINT32, "label": STRING})

    io = BitSocketServer(port=5000)

    secure = io.of("/secure-gateway")
    secure.schema(sync_schema)

    async def auth_middleware(sock, next_fn):
        token = sock.handshake.headers.get("X-Auth-Token")
        if token == "enterprise-payload-passkey":
            next_fn(None)
        else:
            next_fn(Exception("ERR_AUTH_FAILURE"))

    secure.use(auth_middleware)

    async def on_connection(sock):
        print(f"[secure-gateway] socket connected: {sock.id}")

        async def on_provision(payload, ack):
            endpoints = payload.get("endpoints", {})
            create_user = endpoints.get("createUser", {})
            server_type = create_user.get("serverType")

            sock.join(server_type)
            await io.to("GRPC", "/secure-gateway").emit("cluster:sync", {"status": "provisioned"})

            if ack:
                await ack({"status": 200})

        sock.on("cluster:provision", on_provision)

        async def on_sync(payload, ack):
            await sock.to("GRPC").emit("cluster:sync", payload)

        sock.on(sync_schema, on_sync)

    secure.on_connection(on_connection)

    await io.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

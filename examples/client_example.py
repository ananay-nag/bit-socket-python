"""Example BitSocket client connecting to a secured namespace, listening
for schema-synced events, and emitting with an ack callback. A Python port
of bit-socket-node's test/manual/client-app.js.

Run with: python3 examples/client_example.py
"""

import asyncio
import logging

from bitsocket.client import BitSocketClient

logging.basicConfig(level=logging.INFO)


async def main():
    headers = {"X-Auth-Token": "enterprise-payload-passkey"}

    client = BitSocketClient("ws://localhost:5000", nsp="/secure-gateway", headers=headers)

    async def on_connect(payload):
        print("[client] connected")

        async def on_ack(resp):
            print(f"[client] provision ack: {resp}")

        await client.emit(
            "cluster:provision",
            {"endpoints": {"createUser": {"serverType": "GRPC"}}},
            on_ack,
        )

    client.on("connect", on_connect)

    async def on_sync(payload):
        print(f"[client] cluster:sync: {payload}")

    client.on("cluster:sync", on_sync)

    async def on_disconnect(payload):
        print("[client] disconnected")

    client.on("disconnect", on_disconnect)

    async def on_reconnecting(attempt):
        print(f"[client] reconnecting, attempt {attempt}")

    client.on("reconnecting", on_reconnecting)

    await client.connect()
    await asyncio.sleep(10)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

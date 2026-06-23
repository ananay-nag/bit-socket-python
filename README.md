<div align="center">
  <h1>⚡ BitSocket</h1>
  <p><strong>A high-performance, schema-driven, binary WebSocket framework for Python.</strong></p>
  <p>BitSocket provides the developer experience of Socket.io but with <b>Protobuf-level network compression</b>. By leveraging a strict Schema Engine, BitSocket drops JSON completely, stripping keys and formatting to deliver up to an 80% reduction in network payload size.</p>
</div>

<hr />

## 🚀 Why BitSocket?

Socket.io is built on top of Engine.io, which means it transmits stringified JSON. If you send an array of 100 user objects, the keys `"id"`, `"name"`, and `"email"` are transmitted 100 times. 

**BitSocket completely eliminates this overhead.** 
By defining Schemas on your server, BitSocket maps your Python dictionaries directly into strict binary formats. The keys are never transmitted over the network—only the pure, deeply compressed binary data.

### Features
- 🧬 **Schema Auto-Discovery**: Define your schemas on the server. The moment a client connects, the server pushes the schemas to the client during the handshake. Zero manual schema sharing required!
- 📦 **Extreme Binary Compression**: Drops all JSON overhead resulting in 40% to 80% smaller network payloads.
- 🔄 **Connection Multiplexing**: Share a single underlying TCP connection across multiple isolated Namespaces (e.g. `/user`, `/store`), exactly like Socket.io.
- 👥 **Room Broadcasting**: Full support for group communication (`socket.join('room')`, `io.to('room').emit(...)`).
- ♾️ **Recursive Data Types**: Native support for deeply nested objects and multi-dimensional arrays without losing compression.
- 🧩 **Dynamic MsgPack Fallbacks**: Need to send an arbitrary, unpredictable JSON dictionary? Define the field as `'object'` and BitSocket seamlessly drops down to MsgPack compression for that specific field.

---

## 🛠️ Installation

```bash
cd bit-socket-python
pip install -e .
```

Dependencies: `websockets>=13.0`, `msgpack>=1.0`.

---

## 📖 Quick Start

### 1. The Server
Define your schemas, attach them to a namespace, and start listening!

```python
import asyncio
from bitsocket.protocol import Schema, UINT32, STRING
from bitsocket.server import BitSocketServer

async def main():
    # 1. Define strict binary schemas
    sync_schema = Schema("SYNC_TEST", {"id": UINT32, "label": STRING})

    # 2. Initialize Server
    io = BitSocketServer(port=5000)
    
    # 3. Register Schema to namespace "/"
    secure = io.of("/secure-gateway")
    secure.schema(sync_schema)

    async def auth_middleware(sock, next_fn):
        if sock.handshake.headers.get("X-Auth-Token") == "enterprise-payload-passkey":
            next_fn(None)
        else:
            next_fn(Exception("ERR_AUTH_FAILURE"))
    secure.use(auth_middleware)

    # 4. Handle Connections
    async def on_connection(sock):
        sock.join("GRPC")

        async def on_sync(payload, ack):
            await sock.to("GRPC").emit("cluster:sync", payload)
            if ack:
                await ack({"status": 200})
        sock.on(sync_schema, on_sync)

    secure.on_connection(on_connection)
    await io.serve_forever()

asyncio.run(main())
```

### 2. The Client
The client only needs to connect. **It automatically downloads the schemas during the connection handshake!**

```python
import asyncio
from bitsocket.client import BitSocketClient

async def main():
    client = BitSocketClient(
        "ws://localhost:5000",
        nsp="/secure-gateway",
        headers={"X-Auth-Token": "enterprise-payload-passkey"},
    )

    async def on_connect(payload):
        schema = client.get_schema("SYNC_TEST")  # auto-synced from the server

        async def on_ack(resp):
            print("ack:", resp)

        await client.emit(schema, {"id": 1, "label": "node-1"}, on_ack)

    client.on("connect", on_connect)

    await client.connect()
    await asyncio.sleep(10)
    await client.close()

asyncio.run(main())
```

---

## 🧱 Supported Schema Types

BitSocket currently supports mapping your Python data into the following strict representations:

| Schema Type | Python Type | Byte Size |
|-------------|-------------|-----------|
| `UINT8`     | `int`       | 1 byte    |
| `BOOLEAN`   | `bool`      | 1 byte    |
| `UINT16`    | `int`       | 2 bytes   |
| `UINT32`    | `int`       | 4 bytes   |
| `INT32`     | `int`       | 4 bytes   |
| `FLOAT64`   | `float`     | 8 bytes   |
| `STRING`    | `str`       | 4 bytes (len) + utf8 bytes |
| `BYTES`     | `bytes`     | 4 bytes (len) + buffer |

### Advanced Types
- **Arrays**: Wrap a type in a list. `[STRING]` or `[[UINT8]]`.
- **Nested Objects**: Define a dictionary literal. `{"profile": {"age": UINT8}}`.
- **Dynamic Fallbacks**: Use `OBJECT_ANY`, `ARRAY_ANY`, or `ANY` to allow arbitrary JSON data. BitSocket will compress this specific field using MsgPack while preserving keys.

---

## 🌐 Multiplexing & Rooms

BitSocket matches the elegant routing API of Socket.io:

**Namespaces (Multiplexing)**  
Keep logic separated without opening multiple TCP connections.
```python
chat_nsp = root.of('/chat')
game_nsp = root.of('/game')
```

**Rooms**  
Create isolated communication channels within a namespace.
```python
# Server Side
socket.join('lobby-1')
socket.leave('lobby-1')

# Emit to everyone in the room EXCEPT the sender
await socket.broadcast.to('lobby-1').emit('message', data)

# Emit to everyone in the room INCLUDING the sender
await io.of('/chat').to('lobby-1').emit('message', data)
```

---

## 📈 Performance vs Socket.io
See the `NETWORK_ANALYSIS.md` document for a fully quantified byte-for-byte breakdown. In summary:
- **Single Objects**: ~40% smaller payloads.
- **Large Arrays**: ~50% to 80% smaller payloads.
- **Continuous Metrics**: ~60% smaller payloads.

Because network latency (I/O) is the slowest bottleneck in any real-time system, BitSocket provides lower end-to-end latency for high-frequency applications.

---

## 📦 Version History

For detailed features and run methods of each release, please refer to the VERSION file.

- [**v1.0.0**](./VERSION_V1_0_0.md) (2026-06-23) - Initial stable release containing core BitSocket Python server/client modules, handshake auth, asyncio loops, and dict literal schema support.

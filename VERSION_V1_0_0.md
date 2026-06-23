# BitSocket Python Library Version & Features

## Version Information
- **Current Version**: `1.0.0`
- **Package Name**: `bitsocket`
- **Target Platform**: Python >= 3.10

## Key Features

1. **Asyncio-Powered Core Engine**
   Built on the `websockets` asyncio server/client library. The library uses cooperative task scheduling, yielding automatically to print and parse concurrent socket payloads without thread locks.

2. **Insertion-Ordered Dict Schemas**
   Since Python 3.7+ guarantees dictionary insertion order, schemas are declared using standard nested literal dictionaries (e.g. `{"id": UINT32, "profile": {"age": UINT8}}`), preserving the exact JS literal format.

3. **Handshake Interception & Middleware**
   Supports async namespace connection middlewares (`async def auth_middleware(socket, next_fn)`). The `next_fn` argument is a standard synchronous callback (can carry an Exception block to reject upgrades).

4. **Namespace Multiplexing & Rooms**
   Supports joining/leaving rooms via `socket.join(room_name)` / `socket.leave(room_name)` and broadcasts scoping with `await io.emit(...)` or `await socket.to(room_name).emit(...)`.

## Example Run Methods

The library provides examples inside the `examples/` directory:

### 1. Run the Example Python Server
To run the server example that serves on port `5000` and configures namespaces, handshake auth, and schema handling:
```bash
cd bit-socket-python
python3 examples/server_example.py
```

### 2. Run the Example Python Client
To run the client example that connects and emits schema-synced payloads with acknowledgment callbacks:
```bash
cd bit-socket-python
python3 examples/client_example.py
```

### 3. Run the Automated Tests
Run unit tests checking protocol codecs, schema packing/unpacking, and client-server multiplexed connections:
```bash
cd bit-socket-python
pip install -e ".[dev]" pytest-asyncio
pytest
```

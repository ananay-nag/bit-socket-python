"""bitsocket - a Python port of bit-socket-node.

A binary WebSocket protocol with namespaces, rooms, connection/event
middleware, ack callbacks, and optional fixed-layout binary Schemas that
auto-sync from server to client.

    from bitsocket.server import BitSocketServer
    from bitsocket.client import BitSocketClient
    from bitsocket.protocol import Schema
"""

__version__ = "1.0.0"

__all__ = ["__version__"]

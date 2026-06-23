"""bitsocket.client - BitSocketClient, ClientNamespace.

A Python port of bit-socket-node's src/client, built on asyncio + the
`websockets` library, preserving the wire format, namespace multiplexing,
ack-callback, schema auto-sync, and reconnect/heartbeat semantics of the
original.
"""

from .client import BitSocketClient
from .namespace import ClientNamespace

__all__ = ["BitSocketClient", "ClientNamespace"]

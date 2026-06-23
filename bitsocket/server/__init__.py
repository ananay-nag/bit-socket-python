"""bitsocket.server - BitSocketServer, Namespace, Socket.

A Python port of bit-socket-node's src/server, built on asyncio + the
`websockets` library, preserving the wire format and namespace/room/
middleware/ack semantics of the original.
"""

from .server import BitSocketServer
from .namespace import Namespace
from .socket import Socket
from .types import Handshake, Emitter

__all__ = ["BitSocketServer", "Namespace", "Socket", "Handshake", "Emitter"]

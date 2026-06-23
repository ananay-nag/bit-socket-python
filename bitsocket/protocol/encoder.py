"""Frame encoding: [type:1][nspLen:1][nsp][eventLen:1][event][ackId:4][payload...]"""

import struct
from typing import Any, Optional

from .codec import Serializers, default_serializers


def encode_frame(
    frame_type: int,
    nsp: str = "/",
    event: str = "",
    ack_id: int = 0,
    payload: Any = None,
    serializers: Optional[Serializers] = None,
) -> bytes:
    serializers = serializers or default_serializers()
    nsp = nsp or "/"
    event = event or ""

    nsp_bytes = nsp.encode("utf-8")
    event_bytes = event.encode("utf-8")

    payload_bytes = b""
    if payload is not None:
        payload_bytes = serializers.encode_payload(payload)

    size = 1 + 1 + len(nsp_bytes) + 1 + len(event_bytes) + 4 + len(payload_bytes)
    buf = bytearray(size)
    offset = 0

    buf[offset] = frame_type
    offset += 1

    buf[offset] = len(nsp_bytes)
    offset += 1
    buf[offset : offset + len(nsp_bytes)] = nsp_bytes
    offset += len(nsp_bytes)

    buf[offset] = len(event_bytes)
    offset += 1
    buf[offset : offset + len(event_bytes)] = event_bytes
    offset += len(event_bytes)

    struct.pack_into(">I", buf, offset, ack_id & 0xFFFFFFFF)
    offset += 4

    if payload_bytes:
        buf[offset : offset + len(payload_bytes)] = payload_bytes

    return bytes(buf)

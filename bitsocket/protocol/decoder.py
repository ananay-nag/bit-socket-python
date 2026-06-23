"""Frame decoding, the inverse of encoder.py's layout."""

import struct
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .codec import Serializers, default_serializers


@dataclass
class FrameHeader:
    type: int
    nsp: str
    event: str
    ack_id: int


@dataclass
class Frame:
    type: int
    nsp: str
    event: str
    ack_id: int
    payload: Any


def decode_frame_header(buf: bytes) -> Tuple[FrameHeader, bytes]:
    buf = bytes(buf)
    offset = 0
    if len(buf) < offset + 1:
        raise ValueError("bitsocket: frame too short (type)")
    frame_type = buf[offset]
    offset += 1

    if len(buf) < offset + 1:
        raise ValueError("bitsocket: frame too short (nsp length)")
    nsp_len = buf[offset]
    offset += 1
    if len(buf) < offset + nsp_len:
        raise ValueError("bitsocket: frame too short (nsp)")
    nsp = buf[offset : offset + nsp_len].decode("utf-8")
    offset += nsp_len

    if len(buf) < offset + 1:
        raise ValueError("bitsocket: frame too short (event length)")
    event_len = buf[offset]
    offset += 1
    if len(buf) < offset + event_len:
        raise ValueError("bitsocket: frame too short (event)")
    event = buf[offset : offset + event_len].decode("utf-8")
    offset += event_len

    if len(buf) < offset + 4:
        raise ValueError("bitsocket: frame too short (ackId)")
    (ack_id,) = struct.unpack_from(">I", buf, offset)
    offset += 4

    return FrameHeader(type=frame_type, nsp=nsp, event=event, ack_id=ack_id), buf[offset:]


def decode_frame(buf: bytes, serializers: Optional[Serializers] = None) -> Frame:
    serializers = serializers or default_serializers()
    header, payload_bytes = decode_frame_header(buf)

    payload = None
    if len(payload_bytes) > 0:
        if serializers.decode_payload_with_event:
            payload = serializers.decode_payload_with_event(payload_bytes, header.event, header.nsp)
        else:
            payload = serializers.decode_payload(payload_bytes)

    return Frame(type=header.type, nsp=header.nsp, event=header.event, ack_id=header.ack_id, payload=payload)

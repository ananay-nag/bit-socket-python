"""bitsocket.protocol - frame encode/decode, the default msgpack+deflate
payload codec, and the binary Schema codec.

A Go and Python port of bit-socket-node's src/protocol, preserving the wire
format byte-for-byte.
"""

from .constants import (
    FRAME_CONNECT,
    FRAME_EVENT,
    FRAME_ACK,
    FRAME_PING,
    FRAME_PONG,
    FRAME_JOIN,
    FRAME_LEAVE,
)
from .codec import (
    Serializers,
    default_serializers,
    default_encode_payload,
    default_decode_payload,
    deflate_raw,
    inflate_raw,
)
from .encoder import encode_frame
from .decoder import decode_frame, decode_frame_header, Frame, FrameHeader
from .schema import (
    Schema,
    SchemaError,
    UINT8,
    UINT16,
    UINT32,
    INT32,
    FLOAT64,
    BOOLEAN,
    STRING,
    BYTES,
    OBJECT_ANY,
    ARRAY_ANY,
    ANY,
)

__all__ = [
    "FRAME_CONNECT",
    "FRAME_EVENT",
    "FRAME_ACK",
    "FRAME_PING",
    "FRAME_PONG",
    "FRAME_JOIN",
    "FRAME_LEAVE",
    "Serializers",
    "default_serializers",
    "default_encode_payload",
    "default_decode_payload",
    "deflate_raw",
    "inflate_raw",
    "encode_frame",
    "decode_frame",
    "decode_frame_header",
    "Frame",
    "FrameHeader",
    "Schema",
    "SchemaError",
    "UINT8",
    "UINT16",
    "UINT32",
    "INT32",
    "FLOAT64",
    "BOOLEAN",
    "STRING",
    "BYTES",
    "OBJECT_ANY",
    "ARRAY_ANY",
    "ANY",
]

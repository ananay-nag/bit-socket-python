"""Default frame-payload codec: MessagePack, then raw DEFLATE.

Raw DEFLATE (no zlib/gzip header) matches what fflate's deflateSync /
inflateSync produce on the Node.js side, and what Go's compress/flate
produces by default. Python's zlib needs wbits=-15 to get the same raw
(headerless) format.
"""

import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import msgpack


def deflate_raw(data: bytes) -> bytes:
    co = zlib.compressobj(level=zlib.Z_DEFAULT_COMPRESSION, wbits=-15)
    return co.compress(data) + co.flush()


def inflate_raw(data: bytes) -> bytes:
    do = zlib.decompressobj(wbits=-15)
    return do.decompress(bytes(data)) + do.flush()


def default_encode_payload(payload: Any) -> bytes:
    packed = msgpack.packb(payload, use_bin_type=True)
    return deflate_raw(packed)


def default_decode_payload(buf: bytes) -> Any:
    raw = inflate_raw(buf)
    return msgpack.unpackb(raw, raw=False)


@dataclass
class Serializers:
    """Controls how a frame's payload bytes are produced/consumed.

    encode_payload/decode_payload form the generic (non-schema) codec. If
    decode_payload_with_event is set, it takes priority over decode_payload
    and receives (buf, event, nsp) so a caller can look up a per-event
    Schema (this is how schema-aware decoding is plugged in by the
    server/client modules without this module needing to know about
    namespaces or schemas).
    """

    encode_payload: Callable[[Any], bytes] = field(default=default_encode_payload)
    decode_payload: Callable[[bytes], Any] = field(default=default_decode_payload)
    decode_payload_with_event: Optional[Callable[[bytes, str, str], Any]] = None


def default_serializers() -> Serializers:
    return Serializers()

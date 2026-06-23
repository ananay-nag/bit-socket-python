"""Schema: a fixed binary layout for an event's payload, skipping the
generic msgpack+deflate envelope entirely.

A schema's `definition` is a plain, JSON-like Python value:

    - a primitive type tag string: 'uint8', 'uint16', 'uint32', 'int32',
      'float64', 'boolean', 'string', 'bytes', plus the dynamic fallbacks
      'object', 'array', 'any' (msgpack-encoded sub-values)
    - a one-element list ``[elementType]`` for an array of elementType
      (mirrors JS's `['string']` schema array syntax)
    - a dict ``{key: type, ...}`` for an object, in declaration order

This mirrors bit-socket-node's schema.js authoring style almost exactly,
since Python dicts (3.7+) preserve insertion order the same way JS object
literals do - no special ordered-field machinery is needed here, unlike the
Go port (where map iteration order is randomized).

Wire layout (purely positional, identical across all three
implementations):

    uint8/boolean     -> 1 byte
    uint16             -> 2 bytes (big-endian)
    uint32/int32       -> 4 bytes (big-endian)
    float64            -> 8 bytes (big-endian)
    string/bytes       -> 4-byte big-endian length prefix + raw bytes
    object/array/any   -> 4-byte big-endian length prefix + msgpack bytes
    [elementType]      -> 4-byte big-endian element count + each element
    {key: type, ...}   -> each field encoded in declaration order, back to back
"""

import re
import struct
from typing import Any, List

import msgpack

_SCHEMA_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")

# Convenience primitive type tag constants.
UINT8 = "uint8"
UINT16 = "uint16"
UINT32 = "uint32"
INT32 = "int32"
FLOAT64 = "float64"
BOOLEAN = "boolean"
STRING = "string"
BYTES = "bytes"
OBJECT_ANY = "object"
ARRAY_ANY = "array"
ANY = "any"


class SchemaError(Exception):
    pass


class Schema:
    def __init__(self, name: str, definition: Any):
        if not name:
            name = "unknown"
        elif not _SCHEMA_NAME_RE.match(name):
            raise SchemaError(
                f"Invalid Schema Name: '{name}'. Schema names must be a single word "
                "containing only letters, numbers, and underscores (no spaces or "
                "special characters)."
            )
        self.name = name
        self.definition = definition

    def encode_payload(self, payload: Any) -> bytes:
        queue: List[bytes] = []
        size = _compute_size(self.definition, payload, queue)
        buf = bytearray(size)
        _encode_value(self.definition, payload, buf, 0, queue, [0])
        return bytes(buf)

    def decode_payload(self, buf: bytes) -> Any:
        value, _ = _decode_value(self.definition, bytes(buf), 0)
        return value

    def __repr__(self) -> str:
        return f"Schema(name={self.name!r})"


def _compute_size(type_def: Any, val: Any, queue: List[bytes]) -> int:
    if isinstance(type_def, str):
        if type_def in (UINT8, BOOLEAN):
            return 1
        if type_def == UINT16:
            return 2
        if type_def in (UINT32, INT32):
            return 4
        if type_def == FLOAT64:
            return 8
        if type_def == STRING:
            b = (val or "").encode("utf-8")
            queue.append(b)
            return 4 + len(b)
        if type_def == BYTES:
            b = bytes(val) if val else b""
            return 4 + len(b)
        if type_def in (OBJECT_ANY, ARRAY_ANY, ANY):
            packed = msgpack.packb(val, use_bin_type=True)
            queue.append(packed)
            return 4 + len(packed)
        raise SchemaError(f"bitsocket schema error: unsupported type '{type_def}'")

    if isinstance(type_def, list):
        elem_type = type_def[0]
        arr = val or []
        size = 4
        for item in arr:
            size += _compute_size(elem_type, item, queue)
        return size

    if isinstance(type_def, dict):
        size = 0
        obj = val or {}
        for key, sub_type in type_def.items():
            size += _compute_size(sub_type, obj.get(key), queue)
        return size

    raise SchemaError("bitsocket schema error: invalid schema type definition")


def _encode_value(type_def: Any, val: Any, buf: bytearray, offset: int, queue: List[bytes], qidx: List[int]) -> int:
    if isinstance(type_def, str):
        if type_def == UINT8:
            buf[offset] = int(val or 0) & 0xFF
            return offset + 1
        if type_def == BOOLEAN:
            buf[offset] = 1 if val else 0
            return offset + 1
        if type_def == UINT16:
            struct.pack_into(">H", buf, offset, int(val or 0) & 0xFFFF)
            return offset + 2
        if type_def == UINT32:
            struct.pack_into(">I", buf, offset, int(val or 0) & 0xFFFFFFFF)
            return offset + 4
        if type_def == INT32:
            struct.pack_into(">i", buf, offset, int(val or 0))
            return offset + 4
        if type_def == FLOAT64:
            struct.pack_into(">d", buf, offset, float(val or 0))
            return offset + 8
        if type_def in (STRING, OBJECT_ANY, ARRAY_ANY, ANY):
            enc = queue[qidx[0]]
            qidx[0] += 1
            struct.pack_into(">I", buf, offset, len(enc))
            offset += 4
            buf[offset : offset + len(enc)] = enc
            return offset + len(enc)
        if type_def == BYTES:
            b = bytes(val) if val else b""
            struct.pack_into(">I", buf, offset, len(b))
            offset += 4
            buf[offset : offset + len(b)] = b
            return offset + len(b)
        raise SchemaError(f"bitsocket schema error: unsupported type '{type_def}'")

    if isinstance(type_def, list):
        elem_type = type_def[0]
        arr = val or []
        struct.pack_into(">I", buf, offset, len(arr))
        offset += 4
        for item in arr:
            offset = _encode_value(elem_type, item, buf, offset, queue, qidx)
        return offset

    if isinstance(type_def, dict):
        obj = val or {}
        for key, sub_type in type_def.items():
            offset = _encode_value(sub_type, obj.get(key), buf, offset, queue, qidx)
        return offset

    raise SchemaError("bitsocket schema error: invalid schema type definition")


def _decode_value(type_def: Any, buf: bytes, offset: int):
    if isinstance(type_def, str):
        if type_def == UINT8:
            return buf[offset], offset + 1
        if type_def == BOOLEAN:
            return buf[offset] != 0, offset + 1
        if type_def == UINT16:
            return struct.unpack_from(">H", buf, offset)[0], offset + 2
        if type_def == UINT32:
            return struct.unpack_from(">I", buf, offset)[0], offset + 4
        if type_def == INT32:
            return struct.unpack_from(">i", buf, offset)[0], offset + 4
        if type_def == FLOAT64:
            return struct.unpack_from(">d", buf, offset)[0], offset + 8
        if type_def == STRING:
            (n,) = struct.unpack_from(">I", buf, offset)
            offset += 4
            val = buf[offset : offset + n].decode("utf-8")
            return val, offset + n
        if type_def == BYTES:
            (n,) = struct.unpack_from(">I", buf, offset)
            offset += 4
            val = buf[offset : offset + n]
            return val, offset + n
        if type_def in (OBJECT_ANY, ARRAY_ANY, ANY):
            (n,) = struct.unpack_from(">I", buf, offset)
            offset += 4
            sub = buf[offset : offset + n]
            val = msgpack.unpackb(sub, raw=False)
            return val, offset + n
        raise SchemaError(f"bitsocket schema error: unsupported type '{type_def}'")

    if isinstance(type_def, list):
        elem_type = type_def[0]
        (n,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        arr = []
        for _ in range(n):
            v, offset = _decode_value(elem_type, buf, offset)
            arr.append(v)
        return arr, offset

    if isinstance(type_def, dict):
        obj = {}
        for key, sub_type in type_def.items():
            v, offset = _decode_value(sub_type, buf, offset)
            obj[key] = v
        return obj, offset

    raise SchemaError("bitsocket schema error: invalid schema type definition")

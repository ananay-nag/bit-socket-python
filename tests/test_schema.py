import pytest

from bitsocket.protocol import (
    ANY,
    ARRAY_ANY,
    BOOLEAN,
    BYTES,
    FLOAT64,
    INT32,
    OBJECT_ANY,
    STRING,
    UINT8,
    UINT16,
    UINT32,
    Schema,
    SchemaError,
)


def test_schema_basic_types():
    user_schema = Schema("USER_TEST", {"id": UINT32, "name": STRING, "isActive": BOOLEAN})
    payload = {"id": 1045, "name": "Ana", "isActive": True}

    buf = user_schema.encode_payload(payload)
    # 4 (uint32) + 4 (string length) + 3 ("Ana") + 1 (boolean) = 12 bytes
    assert len(buf) == 12

    decoded = user_schema.decode_payload(buf)
    assert decoded == payload


def test_schema_empty_strings_and_bytes():
    edge_schema = Schema("EDGE_TEST", {"buf": BYTES, "text": STRING})
    payload = {"buf": b"", "text": ""}

    buf = edge_schema.encode_payload(payload)
    assert len(buf) == 8

    decoded = edge_schema.decode_payload(buf)
    assert decoded["text"] == ""
    assert decoded["buf"] == b""


def test_schema_all_numeric_types():
    num_schema = Schema(
        "NUM_TEST",
        {"u8": UINT8, "u16": UINT16, "u32": UINT32, "i32": INT32, "f64": FLOAT64},
    )
    payload = {
        "u8": 255,
        "u16": 65535,
        "u32": 4294967295,
        "i32": -2147483648,
        "f64": 3.14159265359,
    }

    buf = num_schema.encode_payload(payload)
    assert len(buf) == 19

    decoded = num_schema.decode_payload(buf)
    assert decoded["u8"] == 255
    assert decoded["u16"] == 65535
    assert decoded["u32"] == 4294967295
    assert decoded["i32"] == -2147483648
    assert decoded["f64"] == pytest.approx(3.14159265359)


def test_schema_nested_objects():
    nested_schema = Schema(
        "NESTED_TEST",
        {"id": UINT32, "profile": {"age": UINT8, "isActive": BOOLEAN}},
    )
    payload = {"id": 999, "profile": {"age": 25, "isActive": True}}

    buf = nested_schema.encode_payload(payload)
    assert len(buf) == 6

    decoded = nested_schema.decode_payload(buf)
    assert decoded == payload


def test_schema_arrays():
    arr_schema = Schema("ARR_TEST", {"tags": [STRING], "matrix": [[UINT8]]})
    payload = {"tags": ["alpha", "beta"], "matrix": [[1, 2], [3, 4]]}

    buf = arr_schema.encode_payload(payload)
    decoded = arr_schema.decode_payload(buf)
    assert decoded["tags"] == ["alpha", "beta"]
    assert decoded["matrix"] == [[1, 2], [3, 4]]


def test_schema_dynamic_fallbacks():
    dyn_schema = Schema("DYN_TEST", {"metadata": OBJECT_ANY, "list": ARRAY_ANY})
    payload = {
        "metadata": {"arbitrary": "data", "val": 42},
        "list": [1, "two", {"three": 3}],
    }

    buf = dyn_schema.encode_payload(payload)
    decoded = dyn_schema.decode_payload(buf)
    assert decoded["metadata"]["arbitrary"] == "data"
    assert decoded["list"][1] == "two"


def test_schema_invalid_name():
    with pytest.raises(SchemaError):
        Schema("bad name!", {"x": UINT8})


def test_schema_field_order_preserved():
    # Python dicts preserve insertion order, so encode/decode using a schema
    # whose definition dict was authored with a particular key order must
    # round-trip the SAME order when introspected.
    schema = Schema("ORDER_TEST", {"z_first": UINT8, "a_second": STRING, "m_third": {"nested": FLOAT64}})
    assert list(schema.definition.keys()) == ["z_first", "a_second", "m_third"]

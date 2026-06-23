from bitsocket.protocol import (
    FRAME_EVENT,
    FRAME_PING,
    decode_frame,
    decode_frame_header,
    encode_frame,
)


def test_encode_decode_frame_no_payload():
    encoded = encode_frame(FRAME_EVENT, nsp="/test", event="hello", ack_id=0, payload=None)
    decoded = decode_frame(encoded)
    assert decoded.type == FRAME_EVENT
    assert decoded.nsp == "/test"
    assert decoded.event == "hello"
    assert decoded.ack_id == 0
    assert decoded.payload is None


def test_encode_decode_frame_map_payload():
    payload = {"user": "test", "id": 123}
    encoded = encode_frame(FRAME_EVENT, nsp="/", event="data", ack_id=42, payload=payload)
    decoded = decode_frame(encoded)
    assert decoded.type == FRAME_EVENT
    assert decoded.nsp == "/"
    assert decoded.event == "data"
    assert decoded.ack_id == 42
    assert decoded.payload == payload


def test_decode_frame_header_too_short():
    try:
        decode_frame_header(b"")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_frame_default_namespace():
    buf = encode_frame(FRAME_PING)
    f = decode_frame(buf)
    assert f.nsp == "/"


def test_array_and_nested_roundtrip():
    payload = {"list": [1, "two", {"three": 3}]}
    buf = encode_frame(FRAME_EVENT, event="dyn", payload=payload)
    f = decode_frame(buf)
    assert f.event == "dyn"
    assert f.payload == payload

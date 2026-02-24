from __future__ import annotations

import base64
import json
import struct
from decimal import Decimal
from typing import Any, List


def encode_row(values: List[Any]) -> bytes:
    # JSON keeps the MVP row codec easy to reason about while still producing binary payloads.
    payload = json.dumps(values, separators=(",", ":"), ensure_ascii=False, default=_json_default).encode("utf-8")
    return struct.pack("<I", len(payload)) + payload


def decode_row(blob: bytes) -> List[Any]:
    (size,) = struct.unpack("<I", blob[:4])
    payload = blob[4 : 4 + size]
    return json.loads(payload.decode("utf-8"), object_hook=_json_object_hook)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"__type__": "bytes", "value": base64.b64encode(bytes(value)).decode("ascii")}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _json_object_hook(value: dict[str, Any]) -> Any:
    marker = value.get("__type__")
    if marker == "decimal":
        return Decimal(str(value["value"]))
    if marker == "bytes":
        return base64.b64decode(str(value["value"]))
    return value

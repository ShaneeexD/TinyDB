from __future__ import annotations

import json
import struct
from typing import Any, List


def encode_row(values: List[Any]) -> bytes:
    # JSON keeps the MVP row codec easy to reason about while still producing binary payloads.
    payload = json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return struct.pack("<I", len(payload)) + payload


def decode_row(blob: bytes) -> List[Any]:
    (size,) = struct.unpack("<I", blob[:4])
    payload = blob[4 : 4 + size]
    return json.loads(payload.decode("utf-8"))

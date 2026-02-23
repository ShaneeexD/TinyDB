from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class PageWrite:
    page_id: int
    after_image: bytes


class WAL:
    def __init__(self, db_path: str):
        self.path = f"{db_path}.wal"
        self._active_txn_id: int | None = None
        self._next_txn_id = 1

    def begin(self) -> int:
        if self._active_txn_id is not None:
            raise RuntimeError("Transaction already active")
        txn_id = self._next_txn_id
        self._next_txn_id += 1
        self._active_txn_id = txn_id
        self._append({"type": "BEGIN", "txn_id": txn_id})
        return txn_id

    def log_page_write(self, page_id: int, after_image: bytes) -> None:
        if self._active_txn_id is None:
            raise RuntimeError("No active transaction")
        encoded = base64.b64encode(after_image).decode("ascii")
        self._append(
            {
                "type": "PAGE_WRITE",
                "txn_id": self._active_txn_id,
                "page_id": page_id,
                "after_image": encoded,
            }
        )

    def commit(self) -> None:
        if self._active_txn_id is None:
            return
        self._append({"type": "COMMIT", "txn_id": self._active_txn_id})
        self._active_txn_id = None

    def abort(self) -> None:
        # No ABORT record is required for redo-only recovery; clearing active state is enough.
        self._active_txn_id = None

    def reset(self) -> None:
        open(self.path, "wb").close()

    def recover(self) -> Dict[int, List[PageWrite]]:
        if not os.path.exists(self.path):
            return {}

        txns: Dict[int, List[PageWrite]] = {}
        committed: set[int] = set()
        with open(self.path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                txn_id = int(entry["txn_id"])
                if entry["type"] == "BEGIN":
                    txns.setdefault(txn_id, [])
                elif entry["type"] == "PAGE_WRITE":
                    after = base64.b64decode(entry["after_image"])
                    txns.setdefault(txn_id, []).append(
                        PageWrite(page_id=int(entry["page_id"]), after_image=after)
                    )
                elif entry["type"] == "COMMIT":
                    committed.add(txn_id)

        replay = {tid: txns.get(tid, []) for tid in sorted(committed)}
        self._next_txn_id = (max(txns.keys()) + 1) if txns else 1
        return replay

    def _append(self, entry: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

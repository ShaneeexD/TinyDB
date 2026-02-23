from __future__ import annotations

import json
import os
import struct
from typing import Any, Dict, Optional

from tinydb_engine.wal.wal import WAL

PAGE_SIZE = 4096
MAGIC = b"TINYDB01"


class Pager:
    def __init__(self, path: str, wal: WAL, page_size: int = PAGE_SIZE):
        self.path = path
        self.page_size = page_size
        self.wal = wal
        self._txn_active = False
        self._txn_dirty: Dict[int, bytes] = {}

        self._recover_if_needed()
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            self._init_file()

        self._fh = open(path, "r+b")
        self.header = self._read_header()

    def close(self) -> None:
        self._fh.flush()
        self._fh.close()

    def begin(self) -> None:
        if self._txn_active:
            raise RuntimeError("Transaction already active")
        self.wal.begin()
        self._txn_active = True
        self._txn_dirty = {}

    def commit(self) -> None:
        if not self._txn_active:
            return

        # Commit marker is written before data pages, so redo can restore committed writes.
        self.wal.commit()
        for page_id, page in self._txn_dirty.items():
            self._write_page_direct(page_id, page)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._txn_dirty.clear()
        self._txn_active = False

    def rollback(self) -> None:
        if not self._txn_active:
            return
        self._txn_dirty.clear()
        self.wal.abort()
        self._txn_active = False

    def page_count(self) -> int:
        # next_page_id always tracks allocation frontier.
        return int(self.header["next_page_id"])

    def allocate_page(self) -> int:
        page_id = self.page_count()
        self.header["next_page_id"] = page_id + 1
        self._persist_header()
        self.write_page(page_id, bytes(self.page_size))
        return page_id

    def read_page(self, page_id: int) -> bytes:
        if self._txn_active and page_id in self._txn_dirty:
            return self._txn_dirty[page_id]

        self._fh.seek(page_id * self.page_size)
        data = self._fh.read(self.page_size)
        if len(data) != self.page_size:
            raise ValueError(f"Invalid page read {page_id}")
        return data

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != self.page_size:
            raise ValueError("Invalid page size")

        if self._txn_active:
            self._txn_dirty[page_id] = data
            self.wal.log_page_write(page_id, data)
            return

        self._write_page_direct(page_id, data)

    def metadata(self) -> Dict[str, Any]:
        return dict(self.header.get("metadata", {}))

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        self.header["metadata"] = metadata
        self._persist_header()

    def _init_file(self) -> None:
        header = {
            "magic": MAGIC.decode("ascii"),
            "version": 1,
            "page_size": self.page_size,
            "next_page_id": 1,
            "metadata": {},
        }
        page = self._encode_header_page(header)
        with open(self.path, "wb") as handle:
            handle.write(page)

    def _recover_if_needed(self) -> None:
        replay = self.wal.recover()
        if not replay:
            return

        if not os.path.exists(self.path):
            self._init_file()

        with open(self.path, "r+b") as handle:
            for _txn_id, writes in replay.items():
                for write in writes:
                    handle.seek(write.page_id * self.page_size)
                    handle.write(write.after_image)
            handle.flush()
            os.fsync(handle.fileno())

        self.wal.reset()

    def _read_header(self) -> Dict[str, Any]:
        page = self.read_page(0)
        (size,) = struct.unpack("<I", page[:4])
        payload = page[4 : 4 + size]
        header = json.loads(payload.decode("utf-8"))
        if header.get("magic") != MAGIC.decode("ascii"):
            raise ValueError("Not a tinydb_engine file")
        if header.get("page_size") != self.page_size:
            raise ValueError("Page size mismatch")
        return header

    def _persist_header(self) -> None:
        page = self._encode_header_page(self.header)
        self.write_page(0, page)

    def _encode_header_page(self, header: Dict[str, Any]) -> bytes:
        payload = json.dumps(header, separators=(",", ":")).encode("utf-8")
        if len(payload) + 4 > self.page_size:
            raise ValueError("Header too large")
        out = bytearray(self.page_size)
        out[:4] = struct.pack("<I", len(payload))
        out[4 : 4 + len(payload)] = payload
        return bytes(out)

    def _write_page_direct(self, page_id: int, data: bytes) -> None:
        self._fh.seek(page_id * self.page_size)
        self._fh.write(data)

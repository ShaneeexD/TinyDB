from __future__ import annotations

import bisect
import json
import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from tinydb_engine.storage.pager import PAGE_SIZE, Pager

MAX_KEYS_PER_NODE = 16


@dataclass
class Node:
    is_leaf: bool
    keys: List[Any]
    children: List[int]
    values: List[Tuple[int, int]]


class BTreeIndex:
    """A small persisted B-tree for PRIMARY KEY lookup.

    The implementation stores one node per page as JSON. This is not as compact as a
    production engine, but keeping node serialization explicit makes balancing logic
    easy to validate in tests.
    """

    def __init__(self, pager: Pager, root_page_id: int):
        self.pager = pager
        self.root_page_id = root_page_id

    @classmethod
    def create(cls, pager: Pager) -> "BTreeIndex":
        root = pager.allocate_page()
        idx = cls(pager, root)
        idx._write_node(
            root,
            Node(is_leaf=True, keys=[], children=[], values=[]),
        )
        return idx

    def find(self, key: Any) -> Optional[Tuple[int, int]]:
        node_page = self.root_page_id
        while True:
            node = self._read_node(node_page)
            i = bisect.bisect_left(node.keys, key)
            if node.is_leaf:
                if i < len(node.keys) and node.keys[i] == key:
                    return tuple(node.values[i])
                return None
            node_page = node.children[i]

    def insert(self, key: Any, value: Tuple[int, int]) -> None:
        root = self._read_node(self.root_page_id)
        if len(root.keys) >= MAX_KEYS_PER_NODE:
            new_root_page = self.pager.allocate_page()
            new_root = Node(is_leaf=False, keys=[], children=[self.root_page_id], values=[])
            self._write_node(new_root_page, new_root)
            self._split_child(new_root_page, 0)
            self.root_page_id = new_root_page
        self._insert_non_full(self.root_page_id, key, value)

    def delete(self, key: Any) -> bool:
        # For MVP we implement delete only at leaf level by traversing to the key location.
        # This keeps write paths predictable and is sufficient for moderate test sizes.
        parent_page = None
        child_index = None
        node_page = self.root_page_id
        while True:
            node = self._read_node(node_page)
            i = bisect.bisect_left(node.keys, key)
            if node.is_leaf:
                if i < len(node.keys) and node.keys[i] == key:
                    node.keys.pop(i)
                    node.values.pop(i)
                    self._write_node(node_page, node)
                    return True
                return False
            parent_page = node_page
            child_index = i
            node_page = node.children[i]

    def scan_items(self) -> List[Tuple[Any, Tuple[int, int]]]:
        items: List[Tuple[Any, Tuple[int, int]]] = []
        self._collect(self.root_page_id, items)
        return items

    def _collect(self, page_id: int, out: List[Tuple[Any, Tuple[int, int]]]) -> None:
        node = self._read_node(page_id)
        if node.is_leaf:
            out.extend((k, tuple(v)) for k, v in zip(node.keys, node.values))
            return
        for child in node.children:
            self._collect(child, out)

    def _insert_non_full(self, page_id: int, key: Any, value: Tuple[int, int]) -> None:
        node = self._read_node(page_id)
        if node.is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            if idx < len(node.keys) and node.keys[idx] == key:
                raise ValueError("Duplicate primary key")
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
            self._write_node(page_id, node)
            return

        idx = bisect.bisect_left(node.keys, key)
        child_page = node.children[idx]
        child = self._read_node(child_page)
        if len(child.keys) >= MAX_KEYS_PER_NODE:
            self._split_child(page_id, idx)
            node = self._read_node(page_id)
            if key > node.keys[idx]:
                idx += 1
        self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent_page: int, child_index: int) -> None:
        parent = self._read_node(parent_page)
        child_page = parent.children[child_index]
        child = self._read_node(child_page)

        mid = len(child.keys) // 2
        median_key = child.keys[mid]

        new_page = self.pager.allocate_page()
        if child.is_leaf:
            left_keys = child.keys[:mid]
            left_vals = child.values[:mid]
            right_keys = child.keys[mid:]
            right_vals = child.values[mid:]

            child.keys = left_keys
            child.values = left_vals
            right = Node(is_leaf=True, keys=right_keys, children=[], values=right_vals)
        else:
            left_keys = child.keys[:mid]
            right_keys = child.keys[mid + 1 :]
            left_children = child.children[: mid + 1]
            right_children = child.children[mid + 1 :]

            child.keys = left_keys
            child.children = left_children
            right = Node(
                is_leaf=False,
                keys=right_keys,
                children=right_children,
                values=[],
            )

        parent.keys.insert(child_index, median_key)
        parent.children.insert(child_index + 1, new_page)

        self._write_node(child_page, child)
        self._write_node(new_page, right)
        self._write_node(parent_page, parent)

    def _read_node(self, page_id: int) -> Node:
        raw = self.pager.read_page(page_id)
        (size,) = struct.unpack("<I", raw[:4])
        payload = json.loads(raw[4 : 4 + size].decode("utf-8")) if size else {}
        return Node(
            is_leaf=payload.get("is_leaf", True),
            keys=payload.get("keys", []),
            children=payload.get("children", []),
            values=[tuple(v) for v in payload.get("values", [])],
        )

    def _write_node(self, page_id: int, node: Node) -> None:
        payload = json.dumps(
            {
                "is_leaf": node.is_leaf,
                "keys": node.keys,
                "children": node.children,
                "values": node.values,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) + 4 > PAGE_SIZE:
            raise ValueError("B-tree node too large for page")
        page = bytearray(PAGE_SIZE)
        page[:4] = struct.pack("<I", len(payload))
        page[4 : 4 + len(payload)] = payload
        self.pager.write_page(page_id, bytes(page))

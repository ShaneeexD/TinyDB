from __future__ import annotations

import struct
from typing import Any, Dict, List, Sequence, Tuple

from tinydb_engine.ast_nodes import (
    AlterTableAddColumnStmt,
    AlterTableRemoveColumnStmt,
    AlterTableRenameColumnStmt,
    AlterTableRenameStmt,
    CreateTableStmt,
    DeleteStmt,
    DropTableStmt,
    InsertStmt,
    SelectStmt,
    Statement,
    UpdateStmt,
    WhereClause,
)
from tinydb_engine.index.btree import BTreeIndex
from tinydb_engine.schema import ColumnSchema, TableSchema, coerce_value, normalize_type
from tinydb_engine.storage.catalog import Catalog
from tinydb_engine.storage.pager import PAGE_SIZE, Pager
from tinydb_engine.storage.record import decode_row, encode_row

SLOT_STRUCT = struct.Struct("<HHH")
PAGE_HEADER_STRUCT = struct.Struct("<HH")


class Executor:
    def __init__(self, pager: Pager):
        self.pager = pager
        self.catalog = Catalog(pager)
        self.schemas: Dict[str, TableSchema] = self.catalog.load()

    def execute(self, statement: Statement) -> Any:
        if isinstance(statement, CreateTableStmt):
            return self._create_table(statement)
        if isinstance(statement, AlterTableRenameStmt):
            return self._alter_table_rename(statement)
        if isinstance(statement, AlterTableRenameColumnStmt):
            return self._alter_table_rename_column(statement)
        if isinstance(statement, AlterTableAddColumnStmt):
            return self._alter_table_add_column(statement)
        if isinstance(statement, AlterTableRemoveColumnStmt):
            return self._alter_table_remove_column(statement)
        if isinstance(statement, DropTableStmt):
            return self._drop_table(statement)
        if isinstance(statement, InsertStmt):
            return self._insert(statement)
        if isinstance(statement, SelectStmt):
            return self._select(statement)
        if isinstance(statement, UpdateStmt):
            return self._update(statement)
        if isinstance(statement, DeleteStmt):
            return self._delete(statement)
        raise ValueError("Unsupported statement")

    def _create_table(self, stmt: CreateTableStmt) -> str:
        key = stmt.table_name.lower()
        if key in self.schemas:
            raise ValueError(f"Table already exists: {stmt.table_name}")

        columns = [
            ColumnSchema(
                name=col.name,
                data_type=normalize_type(col.data_type),
                primary_key=col.primary_key,
                not_null=col.not_null,
            )
            for col in stmt.columns
        ]

        pk_count = sum(1 for c in columns if c.primary_key)
        if pk_count > 1:
            raise ValueError("Only one PRIMARY KEY is supported")

        foreign_keys: List[dict[str, str]] = []
        for local_column, ref_table, ref_column in stmt.foreign_keys:
            if not any(col.name.lower() == local_column.lower() for col in columns):
                raise ValueError(f"Unknown column '{local_column}' in FOREIGN KEY")

            ref_schema = self.schemas.get(ref_table.lower())
            if ref_schema is None:
                raise ValueError(f"Unknown referenced table: {ref_table}")
            if not any(col.name.lower() == ref_column.lower() for col in ref_schema.columns):
                raise ValueError(f"Unknown referenced column: {ref_table}.{ref_column}")

            foreign_keys.append(
                {
                    "column": local_column,
                    "ref_table": ref_table,
                    "ref_column": ref_column,
                }
            )

        index = BTreeIndex.create(self.pager)
        data_page = self._new_table_page()
        schema = TableSchema(
            name=stmt.table_name,
            columns=columns,
            data_page_ids=[data_page],
            pk_index_root_page=index.root_page_id,
            foreign_keys=foreign_keys,
        )
        self.schemas[key] = schema
        self.catalog.save(self.schemas)
        return "OK"

    def _alter_table_remove_column(self, stmt: AlterTableRemoveColumnStmt) -> str:
        schema = self._schema(stmt.table_name)
        remove_idx = schema.column_index(stmt.column_name)

        if len(schema.columns) == 1:
            raise ValueError("Cannot remove the only column")
        if schema.columns[remove_idx].primary_key:
            raise ValueError("Cannot remove PRIMARY KEY column")
        if remove_idx != len(schema.columns) - 1:
            raise ValueError("ALTER TABLE REMOVE COLUMN currently supports only the last column")

        del schema.columns[remove_idx]
        self.catalog.save(self.schemas)
        return "OK"

    def _drop_table(self, stmt: DropTableStmt) -> str:
        key = stmt.table_name.lower()
        if key not in self.schemas:
            raise ValueError(f"Unknown table: {stmt.table_name}")
        del self.schemas[key]
        self.catalog.save(self.schemas)
        return "OK"

    def _alter_table_rename(self, stmt: AlterTableRenameStmt) -> str:
        old_key = stmt.table_name.lower()
        if old_key not in self.schemas:
            raise ValueError(f"Unknown table: {stmt.table_name}")

        new_key = stmt.new_table_name.lower()
        if new_key in self.schemas:
            raise ValueError(f"Table already exists: {stmt.new_table_name}")

        schema = self.schemas.pop(old_key)
        schema.name = stmt.new_table_name
        self.schemas[new_key] = schema
        self.catalog.save(self.schemas)
        return "OK"

    def _alter_table_rename_column(self, stmt: AlterTableRenameColumnStmt) -> str:
        schema = self._schema(stmt.table_name)
        old_idx = schema.column_index(stmt.old_column_name)

        for col in schema.columns:
            if col.name.lower() == stmt.new_column_name.lower():
                raise ValueError(f"Column already exists: {stmt.new_column_name}")

        schema.columns[old_idx].name = stmt.new_column_name
        self.catalog.save(self.schemas)
        return "OK"

    def _alter_table_add_column(self, stmt: AlterTableAddColumnStmt) -> str:
        schema = self._schema(stmt.table_name)

        for col in schema.columns:
            if col.name.lower() == stmt.column.name.lower():
                raise ValueError(f"Column already exists: {stmt.column.name}")

        if stmt.column.primary_key:
            raise ValueError("ALTER TABLE ADD COLUMN does not support PRIMARY KEY")
        if stmt.column.not_null:
            raise ValueError("ALTER TABLE ADD COLUMN does not support NOT NULL")

        schema.columns.append(
            ColumnSchema(
                name=stmt.column.name,
                data_type=normalize_type(stmt.column.data_type),
                primary_key=False,
                not_null=False,
            )
        )
        self.catalog.save(self.schemas)
        return "OK"

    def _insert(self, stmt: InsertStmt) -> str:
        schema = self._schema(stmt.table_name)
        pk_col = schema.pk_column
        pk_idx = schema.column_index(pk_col.name) if pk_col is not None else None
        btree = BTreeIndex(self.pager, schema.pk_index_root_page) if pk_col is not None else None

        for raw_row in stmt.values:
            values = self._materialize_insert_values(schema, stmt.columns, list(raw_row))
            values = self._coerce_row(schema, values)

            if pk_idx is not None and btree is not None:
                pk_val = values[pk_idx]
                if pk_val is None:
                    raise ValueError("PRIMARY KEY cannot be NULL")
                if btree.find(pk_val) is not None:
                    raise ValueError("Duplicate primary key")

            self._validate_foreign_keys(schema, values)

            page_id, slot_id = self._insert_row(schema, values)
            if pk_idx is not None and btree is not None:
                btree.insert(values[pk_idx], (page_id, slot_id))

        if pk_idx is not None and btree is not None:
            schema.pk_index_root_page = btree.root_page_id
            self.catalog.save(self.schemas)
        return "OK"

    def _select(self, stmt: SelectStmt) -> List[Dict[str, Any]]:
        schema = self._schema(stmt.table_name)
        rows = self._select_pk_fast_path(schema, stmt)
        if rows is None:
            rows = self._scan_rows(schema)

        if stmt.where:
            rows = [row for row in rows if self._matches_where(schema, row["values"], stmt.where)]

        if stmt.order_by:
            col, direction = stmt.order_by
            col_idx = schema.column_index(col)
            reverse = direction.upper() == "DESC"
            rows.sort(key=lambda r: (r["values"][col_idx] is None, r["values"][col_idx]), reverse=reverse)

        if stmt.limit is not None:
            rows = rows[: stmt.limit]

        if stmt.columns == ["*"]:
            col_names = [col.name for col in schema.columns]
            return [dict(zip(col_names, row["values"])) for row in rows]

        out_cols = [name for name in stmt.columns]
        indices = [schema.column_index(name) for name in out_cols]
        return [{c: row["values"][i] for c, i in zip(out_cols, indices)} for row in rows]

    def _select_pk_fast_path(self, schema: TableSchema, stmt: SelectStmt) -> List[Dict[str, Any]] | None:
        pk_col = schema.pk_column
        if pk_col is None or stmt.where is None:
            return None

        # Fast path is intentionally narrow: single predicate "pk = value" and no reordering.
        if stmt.order_by is not None:
            return None
        if len(stmt.where.predicates) != 1:
            return None

        col_name, op, raw_value = stmt.where.predicates[0]
        if op != "=" or col_name.lower() != pk_col.name.lower() or raw_value is None:
            return None

        pk_value = coerce_value(raw_value, pk_col.data_type)
        btree = BTreeIndex(self.pager, schema.pk_index_root_page)
        location = btree.find(pk_value)
        if location is None:
            return []

        page_id, slot_id = location
        row_values = self._read_row_at(schema, page_id, slot_id)
        if row_values is None:
            return []
        return [{"page_id": page_id, "slot_id": slot_id, "values": row_values}]

    def _update(self, stmt: UpdateStmt) -> int:
        schema = self._schema(stmt.table_name)
        rows = self._scan_rows(schema)
        assignment_indices = [(schema.column_index(name), value) for name, value in stmt.assignments]
        affected = 0

        pk_col = schema.pk_column
        pk_idx = schema.column_index(pk_col.name) if pk_col else None
        btree = BTreeIndex(self.pager, schema.pk_index_root_page) if pk_col else None

        for row in rows:
            if stmt.where and not self._matches_where(schema, row["values"], stmt.where):
                continue

            new_values = list(row["values"])
            old_pk = new_values[pk_idx] if pk_idx is not None else None
            for col_idx, raw_value in assignment_indices:
                new_values[col_idx] = raw_value
            new_values = self._coerce_row(schema, new_values)

            if pk_idx is not None:
                new_pk = new_values[pk_idx]
                if new_pk is None:
                    raise ValueError("PRIMARY KEY cannot be NULL")
                if new_pk != old_pk and btree and btree.find(new_pk) is not None:
                    raise ValueError("Duplicate primary key")

            self._validate_foreign_keys(schema, new_values)

            page = self.pager.read_page(row["page_id"])
            page_obj = self._read_table_page(page)
            page_obj["slots"][row["slot_id"]]["deleted"] = True
            self.pager.write_page(row["page_id"], self._write_table_page(page_obj))

            new_page, new_slot = self._insert_row(schema, new_values)
            if pk_idx is not None and btree:
                btree.delete(old_pk)
                btree.insert(new_values[pk_idx], (new_page, new_slot))
                schema.pk_index_root_page = btree.root_page_id
            affected += 1

        if affected and pk_idx is not None:
            self.catalog.save(self.schemas)
        return affected

    def _validate_foreign_keys(self, schema: TableSchema, values: List[Any]) -> None:
        for fk in schema.foreign_keys or []:
            local_idx = schema.column_index(fk["column"])
            local_value = values[local_idx]
            if local_value is None:
                continue

            ref_schema = self._schema(fk["ref_table"])
            ref_idx = ref_schema.column_index(fk["ref_column"])
            if not any(row["values"][ref_idx] == local_value for row in self._scan_rows(ref_schema)):
                raise ValueError(
                    f"FOREIGN KEY constraint failed: {schema.name}.{fk['column']} references "
                    f"{ref_schema.name}.{fk['ref_column']}"
                )

    def _assert_not_referenced(self, schema: TableSchema, row_values: List[Any]) -> None:
        for child_schema in self.schemas.values():
            for fk in child_schema.foreign_keys or []:
                if fk["ref_table"].lower() != schema.name.lower():
                    continue

                ref_idx = schema.column_index(fk["ref_column"])
                parent_value = row_values[ref_idx]
                child_idx = child_schema.column_index(fk["column"])
                for child_row in self._scan_rows(child_schema):
                    if child_row["values"][child_idx] == parent_value:
                        raise ValueError(
                            f"FOREIGN KEY constraint failed: row is referenced by "
                            f"{child_schema.name}.{fk['column']}"
                        )

    def _delete(self, stmt: DeleteStmt) -> int:
        schema = self._schema(stmt.table_name)
        rows = self._scan_rows(schema)
        affected = 0
        pk_col = schema.pk_column
        pk_idx = schema.column_index(pk_col.name) if pk_col else None
        btree = BTreeIndex(self.pager, schema.pk_index_root_page) if pk_col else None

        for row in rows:
            if stmt.where and not self._matches_where(schema, row["values"], stmt.where):
                continue
            self._assert_not_referenced(schema, row["values"])
            page = self.pager.read_page(row["page_id"])
            page_obj = self._read_table_page(page)
            page_obj["slots"][row["slot_id"]]["deleted"] = True
            self.pager.write_page(row["page_id"], self._write_table_page(page_obj))
            if pk_idx is not None and btree:
                btree.delete(row["values"][pk_idx])
                schema.pk_index_root_page = btree.root_page_id
            affected += 1

        if affected and pk_idx is not None:
            self.catalog.save(self.schemas)
        return affected

    def _schema(self, table_name: str) -> TableSchema:
        key = table_name.lower()
        if key not in self.schemas:
            raise ValueError(f"Unknown table: {table_name}")
        return self.schemas[key]

    def _materialize_insert_values(
        self,
        schema: TableSchema,
        columns: Sequence[str] | None,
        values: List[Any],
    ) -> List[Any]:
        if columns is None:
            if len(values) != len(schema.columns):
                raise ValueError("INSERT value count mismatch")
            return values

        out = [None] * len(schema.columns)
        if len(columns) != len(values):
            raise ValueError("INSERT columns/value count mismatch")
        for col_name, value in zip(columns, values):
            out[schema.column_index(col_name)] = value
        return out

    def _coerce_row(self, schema: TableSchema, values: List[Any]) -> List[Any]:
        if len(values) != len(schema.columns):
            raise ValueError("Row length mismatch")
        out: List[Any] = []
        for column, value in zip(schema.columns, values):
            if value is None:
                if column.not_null:
                    raise ValueError(f"Column '{column.name}' cannot be NULL")
                out.append(None)
                continue
            out.append(coerce_value(value, column.data_type))
        return out

    def _scan_rows(self, schema: TableSchema) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for page_id in schema.data_page_ids:
            page = self._read_table_page(self.pager.read_page(page_id))
            for slot_id, slot in enumerate(page["slots"]):
                if slot["deleted"]:
                    continue
                rows.append(
                    {
                        "page_id": page_id,
                        "slot_id": slot_id,
                        "values": self._align_row_values(schema, decode_row(slot["blob"])),
                    }
                )
        return rows

    def _read_row_at(self, schema: TableSchema, page_id: int, slot_id: int) -> List[Any] | None:
        if page_id not in schema.data_page_ids:
            return None
        page = self._read_table_page(self.pager.read_page(page_id))
        if slot_id < 0 or slot_id >= len(page["slots"]):
            return None
        slot = page["slots"][slot_id]
        if slot["deleted"]:
            return None
        return self._align_row_values(schema, decode_row(slot["blob"]))

    def _align_row_values(self, schema: TableSchema, values: List[Any]) -> List[Any]:
        if len(values) > len(schema.columns):
            return values[: len(schema.columns)]
        if len(values) < len(schema.columns):
            return values + [None] * (len(schema.columns) - len(values))
        return values

    def _insert_row(self, schema: TableSchema, values: List[Any]) -> Tuple[int, int]:
        row_blob = encode_row(values)
        for page_id in schema.data_page_ids:
            page = self._read_table_page(self.pager.read_page(page_id))
            if self._can_fit(page, len(row_blob)):
                slot_id = self._add_slot(page, row_blob)
                self.pager.write_page(page_id, self._write_table_page(page))
                return page_id, slot_id

        page_id = self._new_table_page()
        schema.data_page_ids.append(page_id)
        self.catalog.save(self.schemas)
        page = self._read_table_page(self.pager.read_page(page_id))
        slot_id = self._add_slot(page, row_blob)
        self.pager.write_page(page_id, self._write_table_page(page))
        return page_id, slot_id

    def _new_table_page(self) -> int:
        page_id = self.pager.allocate_page()
        empty = {
            "free_end": PAGE_SIZE,
            "slots": [],
            "payload": bytearray(PAGE_SIZE),
        }
        self.pager.write_page(page_id, self._write_table_page(empty))
        return page_id

    def _can_fit(self, page: Dict[str, Any], blob_size: int) -> bool:
        slot_count = len(page["slots"])
        free_start = PAGE_HEADER_STRUCT.size + (slot_count * SLOT_STRUCT.size)
        required = blob_size + SLOT_STRUCT.size
        return page["free_end"] - free_start >= required

    def _add_slot(self, page: Dict[str, Any], blob: bytes) -> int:
        free_end = page["free_end"]
        offset = free_end - len(blob)
        page["payload"][offset:free_end] = blob
        page["free_end"] = offset
        page["slots"].append({"offset": offset, "length": len(blob), "deleted": False, "blob": blob})
        return len(page["slots"]) - 1

    def _read_table_page(self, raw: bytes) -> Dict[str, Any]:
        free_end, slot_count = PAGE_HEADER_STRUCT.unpack(raw[: PAGE_HEADER_STRUCT.size])
        slots = []
        pos = PAGE_HEADER_STRUCT.size
        for _ in range(slot_count):
            offset, length, flags = SLOT_STRUCT.unpack(raw[pos : pos + SLOT_STRUCT.size])
            pos += SLOT_STRUCT.size
            blob = raw[offset : offset + length]
            slots.append(
                {
                    "offset": offset,
                    "length": length,
                    "deleted": bool(flags & 1),
                    "blob": blob,
                }
            )
        return {"free_end": free_end, "slots": slots, "payload": bytearray(raw)}

    def _write_table_page(self, page: Dict[str, Any]) -> bytes:
        out = bytearray(PAGE_SIZE)
        PAGE_HEADER_STRUCT.pack_into(out, 0, page["free_end"], len(page["slots"]))
        pos = PAGE_HEADER_STRUCT.size
        for slot in page["slots"]:
            flags = 1 if slot["deleted"] else 0
            SLOT_STRUCT.pack_into(out, pos, slot["offset"], slot["length"], flags)
            pos += SLOT_STRUCT.size
            out[slot["offset"] : slot["offset"] + slot["length"]] = slot["blob"]
        return bytes(out)

    def _matches_where(self, schema: TableSchema, values: List[Any], where: WhereClause) -> bool:
        for col_name, op, raw_value in where.predicates:
            idx = schema.column_index(col_name)
            col = schema.columns[idx]
            right = coerce_value(raw_value, col.data_type) if raw_value is not None else None
            left = values[idx]
            if not self._compare(left, op, right):
                return False
        return True

    def _compare(self, left: Any, op: str, right: Any) -> bool:
        if op == "=":
            return left == right
        if op == "!=":
            return left != right
        if left is None or right is None:
            return False
        if op == "<":
            return left < right
        if op == "<=":
            return left <= right
        if op == ">":
            return left > right
        if op == ">=":
            return left >= right
        raise ValueError(f"Unsupported operator: {op}")

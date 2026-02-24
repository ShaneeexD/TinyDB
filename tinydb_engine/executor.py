from __future__ import annotations

import re
import struct
from typing import Any, Dict, List, Sequence, Tuple

from tinydb_engine.ast_nodes import (
    AlterTableAddColumnStmt,
    AlterTableRemoveColumnStmt,
    AlterTableRenameColumnStmt,
    AlterTableRenameStmt,
    CreateIndexStmt,
    DescribeTableStmt,
    CreateTableStmt,
    DeleteStmt,
    DropIndexStmt,
    DropTableStmt,
    ExplainStmt,
    InsertStmt,
    RollbackStmt,
    SelectStmt,
    ShowIndexesStmt,
    ShowTablesStmt,
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
        if isinstance(statement, ShowTablesStmt):
            return self._show_tables()
        if isinstance(statement, ShowIndexesStmt):
            return self._show_indexes(statement)
        if isinstance(statement, DescribeTableStmt):
            return self._describe_table(statement)
        if isinstance(statement, ExplainStmt):
            return self._explain(statement)
        if isinstance(statement, CreateTableStmt):
            return self._create_table(statement)
        if isinstance(statement, CreateIndexStmt):
            return self._create_index(statement)
        if isinstance(statement, DropIndexStmt):
            return self._drop_index(statement)
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

    def _show_tables(self) -> List[Dict[str, Any]]:
        names = sorted(table.name for table in self.schemas.values())
        return [{"table_name": name} for name in names]

    def _show_indexes(self, stmt: ShowIndexesStmt) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for schema in self.schemas.values():
            if stmt.table_name is not None and schema.name.lower() != stmt.table_name.lower():
                continue
            for idx in schema.secondary_indexes or []:
                rows.append(
                    {
                        "index_name": idx["name"],
                        "table_name": schema.name,
                        "column_name": idx["column"],
                    }
                )
        rows.sort(key=lambda r: (r["table_name"].lower(), r["index_name"].lower()))
        return rows

    def _explain(self, stmt: ExplainStmt) -> List[Dict[str, Any]]:
        inner = stmt.statement
        if not isinstance(inner, SelectStmt):
            return [{"plan": f"FULL EXECUTION ({type(inner).__name__})"}]

        schema = self._schema(inner.table_name)
        if inner.join_table is not None:
            return [{"plan": "NESTED LOOP JOIN"}]
        if self._select_pk_fast_path(schema, inner) is not None:
            return [{"plan": "PK INDEX LOOKUP"}]
        if self._select_secondary_index_fast_path(schema, inner) is not None:
            return [{"plan": "SECONDARY INDEX LOOKUP"}]
        if inner.order_by and self._can_use_index_for_order(schema, inner.order_by[0]):
            return [{"plan": "INDEX ORDER SCAN"}]
        return [{"plan": "FULL TABLE SCAN"}]

    def _describe_table(self, stmt: DescribeTableStmt) -> List[Dict[str, Any]]:
        schema = self._schema(stmt.table_name)
        rows: List[Dict[str, Any]] = []
        for fk in schema.foreign_keys or []:
            key = fk["column"].lower()
            rows.append(
                {
                    "name": fk["column"],
                    "data_type": None,
                    "primary_key": False,
                    "not_null": False,
                    "foreign_key": f"{fk['ref_table']}.{fk['ref_column']}",
                    "_key": key,
                }
            )

        fk_by_col = {row["_key"]: row["foreign_key"] for row in rows}
        idx_by_col: Dict[str, List[str]] = {}
        for idx in schema.secondary_indexes or []:
            key = str(idx["column"]).lower()
            idx_by_col.setdefault(key, []).append(str(idx["name"]))

        out: List[Dict[str, Any]] = []
        for col in schema.columns:
            out.append(
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "primary_key": col.primary_key,
                    "not_null": col.not_null,
                    "unique": col.unique,
                    "default": col.default_value,
                    "foreign_key": fk_by_col.get(col.name.lower()),
                    "indexes": idx_by_col.get(col.name.lower(), []),
                }
            )
        return out

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
                unique=col.unique,
                default_value=(
                    coerce_value(col.default_value, normalize_type(col.data_type))
                    if col.default_value is not None
                    else None
                ),
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
            secondary_indexes=[],
        )
        self.schemas[key] = schema
        self.catalog.save(self.schemas)
        return "OK"

    def _drop_index(self, stmt: DropIndexStmt) -> str:
        for schema in self.schemas.values():
            indexes = schema.secondary_indexes or []
            for i, idx in enumerate(indexes):
                if idx["name"].lower() == stmt.index_name.lower():
                    del indexes[i]
                    schema.secondary_indexes = indexes
                    self.catalog.save(self.schemas)
                    return "OK"
        raise ValueError(f"Unknown index: {stmt.index_name}")

    def _alter_table_remove_column(self, stmt: AlterTableRemoveColumnStmt) -> str:
        schema = self._schema(stmt.table_name)
        remove_idx = schema.column_index(stmt.column_name)

        if len(schema.columns) == 1:
            raise ValueError("Cannot remove the only column")
        if schema.columns[remove_idx].primary_key:
            raise ValueError("Cannot remove PRIMARY KEY column")
        if remove_idx != len(schema.columns) - 1:
            raise ValueError("ALTER TABLE REMOVE COLUMN currently supports only the last column")
        if schema.secondary_indexes:
            for idx in schema.secondary_indexes:
                if idx["column"].lower() == stmt.column_name.lower():
                    raise ValueError("Cannot remove a column with an index")

        del schema.columns[remove_idx]
        self.catalog.save(self.schemas)
        return "OK"

    def _create_index(self, stmt: CreateIndexStmt) -> str:
        schema = self._schema(stmt.table_name)
        if schema.secondary_indexes is None:
            schema.secondary_indexes = []
        if any(idx["name"].lower() == stmt.index_name.lower() for idx in schema.secondary_indexes):
            raise ValueError(f"Index already exists: {stmt.index_name}")

        col_idx = schema.column_index(stmt.column_name)
        col = schema.columns[col_idx]

        btree = BTreeIndex.create(self.pager)
        for row in self._scan_rows(schema):
            value = row["values"][col_idx]
            if value is None:
                continue
            btree.insert_non_unique(value, (row["page_id"], row["slot_id"]))

        schema.secondary_indexes.append(
            {
                "name": stmt.index_name,
                "column": col.name,
                "root_page": btree.root_page_id,
            }
        )
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
        for idx in schema.secondary_indexes or []:
            if idx["column"].lower() == stmt.old_column_name.lower():
                idx["column"] = stmt.new_column_name
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

        default_value = stmt.column.default_value
        if default_value is not None:
            default_value = coerce_value(default_value, normalize_type(stmt.column.data_type))

        schema.columns.append(
            ColumnSchema(
                name=stmt.column.name,
                data_type=normalize_type(stmt.column.data_type),
                primary_key=False,
                not_null=False,
                unique=stmt.column.unique,
                default_value=default_value,
            )
        )
        self.catalog.save(self.schemas)
        return "OK"

    def _insert(self, stmt: InsertStmt) -> str:
        schema = self._schema(stmt.table_name)
        pk_col = schema.pk_column
        pk_idx = schema.column_index(pk_col.name) if pk_col is not None else None
        btree = BTreeIndex(self.pager, schema.pk_index_root_page) if pk_col is not None else None
        sec_btrees = self._secondary_btrees(schema)

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
            self._enforce_unique_constraints(schema, values)

            page_id, slot_id = self._insert_row(schema, values)
            if pk_idx is not None and btree is not None:
                btree.insert(values[pk_idx], (page_id, slot_id))
            for idx_meta, sec_btree in sec_btrees:
                col_idx = schema.column_index(idx_meta["column"])
                idx_value = values[col_idx]
                if idx_value is None:
                    continue
                sec_btree.insert_non_unique(idx_value, (page_id, slot_id))

        if pk_idx is not None and btree is not None:
            schema.pk_index_root_page = btree.root_page_id
        for idx_meta, sec_btree in sec_btrees:
            idx_meta["root_page"] = sec_btree.root_page_id
        if pk_idx is not None or sec_btrees:
            self.catalog.save(self.schemas)
        return "OK"

    def _select(self, stmt: SelectStmt) -> List[Dict[str, Any]]:
        if stmt.join_table is not None:
            return self._select_with_join(stmt)

        schema = self._schema(stmt.table_name)
        rows = self._select_pk_fast_path(schema, stmt)
        if rows is None:
            rows = self._select_secondary_index_fast_path(schema, stmt)
        if rows is None:
            rows = self._scan_rows(schema)
        elif stmt.order_by and self._can_use_index_for_order(schema, stmt.order_by[0]):
            col, direction = stmt.order_by
            col_idx = schema.column_index(col)
            reverse = direction.upper() == "DESC"
            rows.sort(key=lambda r: (r["values"][col_idx] is None, r["values"][col_idx]), reverse=reverse)

        if stmt.where:
            rows = [row for row in rows if self._matches_where(schema, row["values"], stmt.where)]

        if stmt.order_by:
            col, direction = stmt.order_by
            col_idx = schema.column_index(col)
            reverse = direction.upper() == "DESC"
            rows.sort(key=lambda r: (r["values"][col_idx] is None, r["values"][col_idx]), reverse=reverse)

        if stmt.limit is not None:
            rows = rows[: stmt.limit]

        if stmt.group_by or any(self._is_aggregate_expr(col) for col in stmt.columns):
            return self._select_with_grouping(schema, rows, stmt)

        if stmt.columns == ["*"]:
            col_names = [col.name for col in schema.columns]
            return [dict(zip(col_names, row["values"])) for row in rows]

        out_cols = [name for name in stmt.columns]
        indices = [schema.column_index(name) for name in out_cols]
        return [{c: row["values"][i] for c, i in zip(out_cols, indices)} for row in rows]

    def _select_with_grouping(self, schema: TableSchema, rows: List[Dict[str, Any]], stmt: SelectStmt) -> List[Dict[str, Any]]:
        if stmt.columns == ["*"]:
            raise ValueError("GROUP BY/aggregates require explicit SELECT columns")

        group_cols = list(stmt.group_by or [])
        grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}

        if group_cols:
            for row in rows:
                key = tuple(row["values"][schema.column_index(col)] for col in group_cols)
                grouped.setdefault(key, []).append(row)
        else:
            grouped[(None,)] = rows

        out: List[Dict[str, Any]] = []
        for group_rows in grouped.values():
            out_row: Dict[str, Any] = {}
            for expr in stmt.columns:
                if self._is_aggregate_expr(expr):
                    out_row[expr] = self._eval_aggregate_expr(schema, group_rows, expr)
                else:
                    col_idx = schema.column_index(expr)
                    out_row[expr] = group_rows[0]["values"][col_idx] if group_rows else None
            out.append(out_row)
        return out

    def _select_with_join(self, stmt: SelectStmt) -> List[Dict[str, Any]]:
        if stmt.join_table is None or stmt.join_left_column is None or stmt.join_right_column is None:
            raise ValueError("JOIN requires table and ON columns")
        if stmt.columns == ["*"]:
            raise ValueError("SELECT * is not supported with JOIN; explicitly select columns")

        left_schema = self._schema(stmt.table_name)
        right_schema = self._schema(stmt.join_table)

        left_rows = self._scan_rows(left_schema)
        right_rows = self._scan_rows(right_schema)

        left_on = self._join_column_name(left_schema, stmt.table_name, stmt.join_left_column)
        right_on = self._join_column_name(right_schema, stmt.join_table, stmt.join_right_column)
        left_on_idx = left_schema.column_index(left_on)
        right_on_idx = right_schema.column_index(right_on)

        joined: List[Dict[str, Any]] = []
        for left_row in left_rows:
            left_value = left_row["values"][left_on_idx]
            matched = False
            for right_row in right_rows:
                if left_value != right_row["values"][right_on_idx]:
                    continue
                matched = True
                merged = self._merge_join_row(
                    stmt.table_name,
                    left_schema,
                    left_row["values"],
                    stmt.join_table,
                    right_schema,
                    right_row["values"],
                )
                if stmt.where and not self._matches_where_join(merged, stmt.where):
                    continue
                joined.append(merged)
            if stmt.join_type.upper() == "LEFT" and not matched:
                merged = self._merge_join_row(
                    stmt.table_name,
                    left_schema,
                    left_row["values"],
                    stmt.join_table,
                    right_schema,
                    [None] * len(right_schema.columns),
                )
                if stmt.where and not self._matches_where_join(merged, stmt.where):
                    continue
                joined.append(merged)

        if stmt.order_by:
            col, direction = stmt.order_by
            reverse = direction.upper() == "DESC"
            order_key = self._resolve_join_column_key(col, stmt.table_name, left_schema, stmt.join_table, right_schema)
            joined.sort(key=lambda r: (r.get(order_key) is None, r.get(order_key)), reverse=reverse)

        if stmt.limit is not None:
            joined = joined[: stmt.limit]

        out: List[Dict[str, Any]] = []
        for row in joined:
            out_row: Dict[str, Any] = {}
            for col in stmt.columns:
                key = self._resolve_join_column_key(col, stmt.table_name, left_schema, stmt.join_table, right_schema)
                out_row[col] = row.get(key)
            out.append(out_row)
        return out

    def _select_pk_fast_path(self, schema: TableSchema, stmt: SelectStmt) -> List[Dict[str, Any]] | None:
        pk_col = schema.pk_column
        if pk_col is None or stmt.where is None:
            return None
        if stmt.join_table is not None:
            return None

        # Fast path is intentionally narrow: single predicate "pk = value" and no reordering.
        if stmt.order_by is not None:
            return None
        if len(stmt.where.groups) != 1 or len(stmt.where.groups[0]) != 1:
            return None

        col_name, op, raw_value = stmt.where.groups[0][0]
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

    def _select_secondary_index_fast_path(self, schema: TableSchema, stmt: SelectStmt) -> List[Dict[str, Any]] | None:
        if stmt.join_table is not None or stmt.where is None:
            return None
        if len(stmt.where.groups) != 1 or len(stmt.where.groups[0]) != 1:
            return None

        col_name, op, raw_value = stmt.where.groups[0][0]
        if op not in {"=", "IN"} or raw_value is None:
            return None

        for idx_meta in schema.secondary_indexes or []:
            if idx_meta["column"].lower() != col_name.lower():
                continue
            col_idx = schema.column_index(idx_meta["column"])
            typed_values = (
                [coerce_value(raw_value, schema.columns[col_idx].data_type)]
                if op == "="
                else [coerce_value(item, schema.columns[col_idx].data_type) for item in (raw_value or [])]
            )
            btree = BTreeIndex(self.pager, int(idx_meta["root_page"]))
            out: List[Dict[str, Any]] = []
            seen: set[Tuple[int, int]] = set()
            for typed_value in typed_values:
                for page_id, slot_id in btree.find_all(typed_value):
                    if (page_id, slot_id) in seen:
                        continue
                    seen.add((page_id, slot_id))
                    row_values = self._read_row_at(schema, page_id, slot_id)
                    if row_values is None:
                        continue
                    out.append({"page_id": page_id, "slot_id": slot_id, "values": row_values})
            return out
        return None

    def _update(self, stmt: UpdateStmt) -> int:
        schema = self._schema(stmt.table_name)
        rows = self._scan_rows(schema)
        assignment_indices = [(schema.column_index(name), value) for name, value in stmt.assignments]
        affected = 0

        pk_col = schema.pk_column
        pk_idx = schema.column_index(pk_col.name) if pk_col else None
        btree = BTreeIndex(self.pager, schema.pk_index_root_page) if pk_col else None
        sec_btrees = self._secondary_btrees(schema)

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
            self._enforce_unique_constraints(schema, new_values, skip_row=(row["page_id"], row["slot_id"]))

            page = self.pager.read_page(row["page_id"])
            page_obj = self._read_table_page(page)
            page_obj["slots"][row["slot_id"]]["deleted"] = True
            self.pager.write_page(row["page_id"], self._write_table_page(page_obj))

            new_page, new_slot = self._insert_row(schema, new_values)
            if pk_idx is not None and btree:
                btree.delete(old_pk)
                btree.insert(new_values[pk_idx], (new_page, new_slot))
                schema.pk_index_root_page = btree.root_page_id
            for idx_meta, sec_btree in sec_btrees:
                col_idx = schema.column_index(idx_meta["column"])
                old_val = row["values"][col_idx]
                new_val = new_values[col_idx]
                if old_val is not None:
                    sec_btree.delete_non_unique(old_val, (row["page_id"], row["slot_id"]))
                if new_val is not None:
                    sec_btree.insert_non_unique(new_val, (new_page, new_slot))
            affected += 1

        if affected:
            for idx_meta, sec_btree in sec_btrees:
                idx_meta["root_page"] = sec_btree.root_page_id
        if affected and (pk_idx is not None or sec_btrees):
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
        sec_btrees = self._secondary_btrees(schema)

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
            for idx_meta, sec_btree in sec_btrees:
                col_idx = schema.column_index(idx_meta["column"])
                old_val = row["values"][col_idx]
                if old_val is not None:
                    sec_btree.delete_non_unique(old_val, (row["page_id"], row["slot_id"]))
            affected += 1

        if affected:
            for idx_meta, sec_btree in sec_btrees:
                idx_meta["root_page"] = sec_btree.root_page_id
        if affected and (pk_idx is not None or sec_btrees):
            self.catalog.save(self.schemas)
        return affected

    def _secondary_btrees(self, schema: TableSchema) -> List[Tuple[dict[str, Any], BTreeIndex]]:
        out: List[Tuple[dict[str, Any], BTreeIndex]] = []
        for idx_meta in schema.secondary_indexes or []:
            out.append((idx_meta, BTreeIndex(self.pager, int(idx_meta["root_page"]))))
        return out

    def _can_use_index_for_order(self, schema: TableSchema, col_name: str) -> bool:
        if schema.pk_column and schema.pk_column.name.lower() == col_name.lower():
            return True
        return any(idx["column"].lower() == col_name.lower() for idx in (schema.secondary_indexes or []))

    def _is_aggregate_expr(self, expr: str) -> bool:
        upper = expr.upper()
        return upper.startswith("COUNT(") or upper.startswith("SUM(") or upper.startswith("AVG(") or upper.startswith("MIN(") or upper.startswith("MAX(")

    def _eval_aggregate_expr(self, schema: TableSchema, rows: List[Dict[str, Any]], expr: str) -> Any:
        upper = expr.upper()
        open_idx = expr.find("(")
        close_idx = expr.rfind(")")
        if open_idx == -1 or close_idx == -1 or close_idx <= open_idx:
            raise ValueError(f"Invalid aggregate expression: {expr}")
        func = upper[:open_idx]
        arg = expr[open_idx + 1 : close_idx].strip()

        if func == "COUNT" and arg == "*":
            return len(rows)

        col_idx = schema.column_index(arg)
        values = [row["values"][col_idx] for row in rows if row["values"][col_idx] is not None]
        if func == "COUNT":
            return len(values)
        if not values:
            return None
        if func == "SUM":
            return sum(values)
        if func == "AVG":
            return sum(values) / len(values)
        if func == "MIN":
            return min(values)
        if func == "MAX":
            return max(values)
        raise ValueError(f"Unsupported aggregate function: {func}")

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
        for idx, column in enumerate(schema.columns):
            if out[idx] is None and column.default_value is not None:
                out[idx] = column.default_value
        return out

    def _enforce_unique_constraints(
        self,
        schema: TableSchema,
        values: List[Any],
        skip_row: Tuple[int, int] | None = None,
    ) -> None:
        unique_indexes = [idx for idx, col in enumerate(schema.columns) if col.unique]
        if not unique_indexes:
            return

        for existing in self._scan_rows(schema):
            if skip_row is not None and (existing["page_id"], existing["slot_id"]) == skip_row:
                continue
            for idx in unique_indexes:
                new_value = values[idx]
                if new_value is None:
                    continue
                if existing["values"][idx] == new_value:
                    raise ValueError(f"UNIQUE constraint failed: {schema.name}.{schema.columns[idx].name}")

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
            out = list(values)
            for idx in range(len(values), len(schema.columns)):
                out.append(schema.columns[idx].default_value)
            return out
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
        for group in where.groups:
            group_matches = True
            for col_name, op, raw_value in group:
                idx = schema.column_index(col_name)
                col = schema.columns[idx]
                left = values[idx]

                if op == "IS NULL":
                    if left is not None:
                        group_matches = False
                        break
                    continue

                if op == "IS NOT NULL":
                    if left is None:
                        group_matches = False
                        break
                    continue

                if op == "IN":
                    if not isinstance(raw_value, list):
                        raise ValueError("IN predicate requires a list of values")
                    right_values = [coerce_value(item, col.data_type) if item is not None else None for item in raw_value]
                    if left not in right_values:
                        group_matches = False
                        break
                    continue

                if op == "NOT IN":
                    if not isinstance(raw_value, list):
                        raise ValueError("NOT IN predicate requires a list of values")
                    right_values = [coerce_value(item, col.data_type) if item is not None else None for item in raw_value]
                    if left in right_values:
                        group_matches = False
                        break
                    continue

                if op == "LIKE":
                    if left is None:
                        group_matches = False
                        break
                    if not isinstance(raw_value, str):
                        raise ValueError("LIKE predicate requires a string pattern")
                    pattern = "^" + re.escape(raw_value).replace(r"%", ".*").replace(r"_", ".") + "$"
                    if re.match(pattern, str(left)) is None:
                        group_matches = False
                        break
                    continue

                right = coerce_value(raw_value, col.data_type) if raw_value is not None else None
                if not self._compare(left, op, right):
                    group_matches = False
                    break

            if group_matches:
                return True
        return False

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

    def _join_column_name(self, schema: TableSchema, table_name: str, identifier: str) -> str:
        if "." in identifier:
            prefix, col_name = identifier.split(".", 1)
            if prefix.lower() != table_name.lower():
                raise ValueError(f"JOIN ON column '{identifier}' does not belong to table '{table_name}'")
            return col_name
        return identifier

    def _merge_join_row(
        self,
        left_table: str,
        left_schema: TableSchema,
        left_values: List[Any],
        right_table: str,
        right_schema: TableSchema,
        right_values: List[Any],
    ) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for idx, col in enumerate(left_schema.columns):
            merged[f"{left_table}.{col.name}"] = left_values[idx]
        for idx, col in enumerate(right_schema.columns):
            merged[f"{right_table}.{col.name}"] = right_values[idx]
        return merged

    def _resolve_join_column_key(
        self,
        identifier: str,
        left_table: str,
        left_schema: TableSchema,
        right_table: str,
        right_schema: TableSchema,
    ) -> str:
        if "." in identifier:
            table, col = identifier.split(".", 1)
            if table.lower() == left_table.lower():
                left_schema.column_index(col)
                return identifier
            if table.lower() == right_table.lower():
                right_schema.column_index(col)
                return identifier
            raise ValueError(f"Unknown table prefix in JOIN column: {identifier}")
        left_key = f"{left_table}.{identifier}"
        right_key = f"{right_table}.{identifier}"

        left_exists = any(col.name.lower() == identifier.lower() for col in left_schema.columns)
        right_exists = any(col.name.lower() == identifier.lower() for col in right_schema.columns)
        if left_exists and right_exists:
            raise ValueError(f"Ambiguous column in JOIN result: {identifier}")
        if left_exists:
            return left_key
        if right_exists:
            return right_key
        raise ValueError(f"Unknown column in JOIN result: {identifier}")

    def _matches_where_join(self, row: Dict[str, Any], where: WhereClause) -> bool:
        for group in where.groups:
            group_matches = True
            for col_name, op, raw_value in group:
                key = col_name if col_name in row else self._resolve_unqualified_join_where_key(col_name, row)
                left = row.get(key)

                if op == "IS NULL":
                    if left is not None:
                        group_matches = False
                        break
                    continue
                if op == "IS NOT NULL":
                    if left is None:
                        group_matches = False
                        break
                    continue
                if op == "IN":
                    if not isinstance(raw_value, list) or left not in raw_value:
                        group_matches = False
                        break
                    continue
                if op == "NOT IN":
                    if not isinstance(raw_value, list) or left in raw_value:
                        group_matches = False
                        break
                    continue
                if op == "LIKE":
                    if left is None or not isinstance(raw_value, str):
                        group_matches = False
                        break
                    pattern = "^" + re.escape(raw_value).replace(r"%", ".*").replace(r"_", ".") + "$"
                    if re.match(pattern, str(left)) is None:
                        group_matches = False
                        break
                    continue
                if not self._compare(left, op, raw_value):
                    group_matches = False
                    break
            if group_matches:
                return True
        return False

    def _resolve_unqualified_join_where_key(self, identifier: str, row: Dict[str, Any]) -> str:
        matches = [key for key in row.keys() if key.endswith(f".{identifier}")]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(f"Ambiguous column in JOIN WHERE: {identifier}")
        raise ValueError(f"Unknown column in JOIN WHERE: {identifier}")

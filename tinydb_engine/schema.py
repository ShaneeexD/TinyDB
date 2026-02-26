from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional


SUPPORTED_TYPES = {"INTEGER", "TEXT", "REAL", "BOOLEAN", "TIMESTAMP", "BLOB", "DECIMAL", "NUMERIC"}


@dataclass
class ColumnSchema:
    name: str
    data_type: str
    primary_key: bool = False
    not_null: bool = False
    unique: bool = False
    default_value: Any = None
    auto_increment: bool = False
    check_exprs: List[str] | None = None


@dataclass
class TableSchema:
    name: str
    columns: List[ColumnSchema]
    data_page_ids: List[int]
    pk_index_root_page: int
    foreign_keys: List[dict[str, str]] | None = None
    secondary_indexes: List[dict[str, Any]] | None = None
    check_exprs: List[str] | None = None

    @property
    def pk_column(self) -> Optional[ColumnSchema]:
        for column in self.columns:
            if column.primary_key:
                return column
        return None

    def column_index(self, name: str) -> int:
        for idx, column in enumerate(self.columns):
            if column.name.lower() == name.lower():
                return idx
        raise KeyError(f"Unknown column '{name}'")


def normalize_type(type_name: str) -> str:
    normalized = type_name.upper()
    if normalized == "NUMERIC":
        normalized = "DECIMAL"
    if normalized not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported type: {type_name}")
    return normalized


def coerce_value(value: Any, data_type: str) -> Any:
    if value is None:
        return None
    if data_type == "INTEGER":
        if isinstance(value, bool):
            return int(value)
        return int(value)
    if data_type == "REAL":
        return float(value)
    if data_type == "TEXT":
        return str(value)
    if data_type == "TIMESTAMP":
        return str(value)
    if data_type == "BLOB":
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        raise ValueError(f"Cannot coerce '{value}' to BLOB")
    if data_type == "DECIMAL":
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            return Decimal(int(value))
        return Decimal(str(value))
    if data_type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1"}:
            return True
        if text in {"false", "0"}:
            return False
        raise ValueError(f"Cannot coerce '{value}' to BOOLEAN")
    raise ValueError(f"Unsupported type: {data_type}")


def serialize_schema_map(schema_map: Dict[str, TableSchema]) -> Dict[str, Any]:
    return {
        name: {
            "name": schema.name,
            "columns": [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "primary_key": col.primary_key,
                    "not_null": col.not_null,
                    "unique": col.unique,
                    "default_value": col.default_value,
                    "auto_increment": col.auto_increment,
                    "check_exprs": list(col.check_exprs or []),
                }
                for col in schema.columns
            ],
            "data_page_ids": schema.data_page_ids,
            "pk_index_root_page": schema.pk_index_root_page,
            "foreign_keys": list(schema.foreign_keys or []),
            "secondary_indexes": list(schema.secondary_indexes or []),
            "check_exprs": list(schema.check_exprs or []),
        }
        for name, schema in schema_map.items()
    }


def deserialize_schema_map(payload: Dict[str, Any]) -> Dict[str, TableSchema]:
    output: Dict[str, TableSchema] = {}
    for name, table in payload.items():
        output[name] = TableSchema(
            name=table["name"],
            columns=[ColumnSchema(**col) for col in table["columns"]],
            data_page_ids=list(table["data_page_ids"]),
            pk_index_root_page=int(table["pk_index_root_page"]),
            foreign_keys=[dict(item) for item in table.get("foreign_keys", [])],
            secondary_indexes=[dict(item) for item in table.get("secondary_indexes", [])],
            check_exprs=list(table.get("check_exprs", [])),
        )
    return output

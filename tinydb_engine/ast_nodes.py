from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class ColumnDef:
    name: str
    data_type: str
    primary_key: bool = False
    not_null: bool = False


@dataclass(frozen=True)
class WhereClause:
    # AND-only predicates keep the MVP parser small and deterministic.
    predicates: List[Tuple[str, str, Any]]


@dataclass(frozen=True)
class CreateTableStmt:
    table_name: str
    columns: Sequence[ColumnDef]


@dataclass(frozen=True)
class InsertStmt:
    table_name: str
    columns: Optional[Sequence[str]]
    values: Sequence[Sequence[Any]]


@dataclass(frozen=True)
class SelectStmt:
    table_name: str
    columns: Sequence[str]
    where: Optional[WhereClause] = None
    order_by: Optional[Tuple[str, str]] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class UpdateStmt:
    table_name: str
    assignments: Sequence[Tuple[str, Any]]
    where: Optional[WhereClause] = None


@dataclass(frozen=True)
class DeleteStmt:
    table_name: str
    where: Optional[WhereClause] = None


@dataclass(frozen=True)
class DropTableStmt:
    table_name: str


@dataclass(frozen=True)
class AlterTableRenameStmt:
    table_name: str
    new_table_name: str


@dataclass(frozen=True)
class AlterTableRenameColumnStmt:
    table_name: str
    old_column_name: str
    new_column_name: str


@dataclass(frozen=True)
class AlterTableAddColumnStmt:
    table_name: str
    column: ColumnDef


@dataclass(frozen=True)
class AlterTableRemoveColumnStmt:
    table_name: str
    column_name: str


Statement = (
    CreateTableStmt
    | InsertStmt
    | SelectStmt
    | UpdateStmt
    | DeleteStmt
    | DropTableStmt
    | AlterTableRenameStmt
    | AlterTableRenameColumnStmt
    | AlterTableAddColumnStmt
    | AlterTableRemoveColumnStmt
)

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple


Predicate = Tuple[str, str, Any]


@dataclass(frozen=True)
class ColumnDef:
    name: str
    data_type: str
    primary_key: bool = False
    not_null: bool = False
    unique: bool = False
    default_value: Any = None
    auto_increment: bool = False
    check_exprs: Sequence[str] = ()


@dataclass(frozen=True)
class WhereClause:
    # OR of AND groups. Each inner list is AND-combined predicates.
    groups: List[List[Predicate]]


@dataclass(frozen=True)
class CreateTableStmt:
    table_name: str
    columns: Sequence[ColumnDef]
    foreign_keys: Sequence[Tuple[str, str, str, str]]
    primary_key_columns: Sequence[str] = ()
    check_exprs: Sequence[str] = ()
    if_not_exists: bool = False


@dataclass(frozen=True)
class InsertStmt:
    table_name: str
    columns: Optional[Sequence[str]]
    values: Sequence[Sequence[Any]]
    or_replace: bool = False


@dataclass(frozen=True)
class SelectStmt:
    table_name: str
    columns: Sequence[str]
    distinct: bool = False
    join_type: str = "INNER"
    join_table: Optional[str] = None
    join_left_column: Optional[str] = None
    join_right_column: Optional[str] = None
    joins: Optional[Sequence["JoinClause"]] = None
    where: Optional[WhereClause] = None
    group_by: Optional[Sequence[str]] = None
    having: Optional[WhereClause] = None
    order_by: Optional[Tuple[str, str]] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class JoinClause:
    join_type: str
    table_name: str
    left_column: str
    right_column: str


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
class CreateIndexStmt:
    index_name: str
    table_name: str
    column_names: Sequence[str]


@dataclass(frozen=True)
class DropIndexStmt:
    index_name: str


@dataclass(frozen=True)
class ShowIndexesStmt:
    table_name: str | None = None


@dataclass(frozen=True)
class ShowStatsStmt:
    pass


@dataclass(frozen=True)
class ExplainStmt:
    statement: "Statement"


@dataclass(frozen=True)
class ProfileStmt:
    statement: "Statement"


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


@dataclass(frozen=True)
class BeginStmt:
    pass


@dataclass(frozen=True)
class CommitStmt:
    pass


@dataclass(frozen=True)
class RollbackStmt:
    pass


@dataclass(frozen=True)
class ShowTablesStmt:
    pass


@dataclass(frozen=True)
class DescribeTableStmt:
    table_name: str


@dataclass(frozen=True)
class ReindexStmt:
    table_name: str


Statement = (
    CreateTableStmt
    | InsertStmt
    | SelectStmt
    | UpdateStmt
    | DeleteStmt
    | DropTableStmt
    | CreateIndexStmt
    | DropIndexStmt
    | ShowIndexesStmt
    | ShowStatsStmt
    | ExplainStmt
    | ProfileStmt
    | AlterTableRenameStmt
    | AlterTableRenameColumnStmt
    | AlterTableAddColumnStmt
    | AlterTableRemoveColumnStmt
    | BeginStmt
    | CommitStmt
    | RollbackStmt
    | ShowTablesStmt
    | DescribeTableStmt
    | ReindexStmt
)

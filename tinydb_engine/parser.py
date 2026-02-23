from __future__ import annotations

import re
from typing import Any, List, Sequence, Tuple

from .ast_nodes import (
    AlterTableAddColumnStmt,
    AlterTableRemoveColumnStmt,
    AlterTableRenameColumnStmt,
    AlterTableRenameStmt,
    ColumnDef,
    CreateTableStmt,
    DeleteStmt,
    DropTableStmt,
    InsertStmt,
    SelectStmt,
    Statement,
    UpdateStmt,
    WhereClause,
)

_TOKEN_RE = re.compile(
    r"\s*(=>|<=|>=|!=|[(),=*<>]|\bAND\b|\bASC\b|\bDESC\b|\bLIMIT\b|\bORDER\b|\bBY\b|\bWHERE\b|\bFROM\b|\bVALUES\b|\bINTO\b|\bTABLE\b|\bCREATE\b|\bINSERT\b|\bSELECT\b|\bUPDATE\b|\bDELETE\b|\bDROP\b|\bSET\b|\bALTER\b|\bRENAME\b|\bADD\b|\bREMOVE\b|\bCOLUMN\b|\bTO\b|\bPRIMARY\b|\bKEY\b|\bNOT\b|\bNULL\b|\*|\bTRUE\b|\bFALSE\b|\bNULL\b|'[^']*'|\d+\.\d+|\d+|[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


class ParseError(ValueError):
    pass


class TokenStream:
    def __init__(self, tokens: Sequence[str]):
        self.tokens = list(tokens)
        self.pos = 0

    def peek(self) -> str | None:
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def pop(self) -> str:
        token = self.peek()
        if token is None:
            raise ParseError("Unexpected end of statement")
        self.pos += 1
        return token

    def expect(self, expected: str) -> str:
        token = self.pop()
        if token.upper() != expected.upper():
            raise ParseError(f"Expected '{expected}', got '{token}'")
        return token

    def consume(self, expected: str) -> bool:
        token = self.peek()
        if token is not None and token.upper() == expected.upper():
            self.pos += 1
            return True
        return False


def tokenize(sql: str) -> List[str]:
    cleaned = sql.strip().rstrip(";")
    if not cleaned:
        raise ParseError("Empty SQL statement")

    tokens: List[str] = []
    pos = 0
    while pos < len(cleaned):
        if cleaned[pos].isspace():
            pos += 1
            continue
        match = _TOKEN_RE.match(cleaned, pos)
        if match is None:
            snippet = cleaned[pos : pos + 24]
            raise ParseError(f"Unsupported SQL syntax near: {snippet!r}")
        tokens.append(match.group(1))
        pos = match.end()

    return tokens


def parse(sql: str) -> Statement:
    stream = TokenStream(tokenize(sql))
    token = stream.peek()
    if token is None:
        raise ParseError("Empty SQL statement")

    keyword = token.upper()
    if keyword == "CREATE":
        return _parse_create(stream)
    if keyword == "INSERT":
        return _parse_insert(stream)
    if keyword == "SELECT":
        return _parse_select(stream)
    if keyword == "UPDATE":
        return _parse_update(stream)
    if keyword == "DELETE":
        return _parse_delete(stream)
    if keyword == "DROP":
        return _parse_drop(stream)
    if keyword == "ALTER":
        return _parse_alter(stream)
    raise ParseError(f"Unsupported command: {keyword}")


def _parse_create(stream: TokenStream) -> CreateTableStmt:
    stream.expect("CREATE")
    stream.expect("TABLE")
    table_name = stream.pop()
    stream.expect("(")

    columns: List[ColumnDef] = []
    while True:
        col_name = stream.pop()
        col_type = stream.pop().upper()
        primary_key = False
        not_null = False
        if stream.consume("PRIMARY"):
            stream.expect("KEY")
            primary_key = True
            not_null = True
        if stream.consume("NOT"):
            stream.expect("NULL")
            not_null = True
        columns.append(
            ColumnDef(
                name=col_name,
                data_type=col_type,
                primary_key=primary_key,
                not_null=not_null,
            )
        )

        if stream.consume(","):
            continue
        stream.expect(")")
        break

    _assert_consumed(stream)
    return CreateTableStmt(table_name=table_name, columns=columns)


def _parse_insert(stream: TokenStream) -> InsertStmt:
    stream.expect("INSERT")
    stream.expect("INTO")
    table_name = stream.pop()

    columns = None
    if stream.consume("("):
        names: List[str] = []
        while True:
            names.append(stream.pop())
            if stream.consume(","):
                continue
            stream.expect(")")
            break
        columns = names

    stream.expect("VALUES")
    values: List[List[Any]] = []
    while True:
        stream.expect("(")
        row_values: List[Any] = []
        while True:
            row_values.append(_parse_literal(stream.pop()))
            if stream.consume(","):
                continue
            stream.expect(")")
            break
        values.append(row_values)
        if stream.consume(","):
            continue
        break

    _assert_consumed(stream)
    return InsertStmt(table_name=table_name, columns=columns, values=values)


def _parse_select(stream: TokenStream) -> SelectStmt:
    stream.expect("SELECT")
    columns: List[str] = []
    if stream.consume("*"):
        columns = ["*"]
    else:
        while True:
            columns.append(stream.pop())
            if stream.consume(","):
                continue
            break

    stream.expect("FROM")
    table_name = stream.pop()

    where = _parse_where(stream)

    order_by = None
    if stream.consume("ORDER"):
        stream.expect("BY")
        col = stream.pop()
        direction = "ASC"
        next_tok = stream.peek()
        if next_tok and next_tok.upper() in {"ASC", "DESC"}:
            direction = stream.pop().upper()
        order_by = (col, direction)

    limit = None
    if stream.consume("LIMIT"):
        limit = int(stream.pop())

    _assert_consumed(stream)
    return SelectStmt(
        table_name=table_name,
        columns=columns,
        where=where,
        order_by=order_by,
        limit=limit,
    )


def _parse_update(stream: TokenStream) -> UpdateStmt:
    stream.expect("UPDATE")
    table_name = stream.pop()
    stream.expect("SET")

    assignments: List[Tuple[str, Any]] = []
    while True:
        name = stream.pop()
        stream.expect("=")
        value = _parse_literal(stream.pop())
        assignments.append((name, value))
        if stream.consume(","):
            continue
        break

    where = _parse_where(stream)
    _assert_consumed(stream)
    return UpdateStmt(table_name=table_name, assignments=assignments, where=where)


def _parse_delete(stream: TokenStream) -> DeleteStmt:
    stream.expect("DELETE")
    stream.expect("FROM")
    table_name = stream.pop()
    where = _parse_where(stream)
    _assert_consumed(stream)
    return DeleteStmt(table_name=table_name, where=where)


def _parse_drop(stream: TokenStream) -> DropTableStmt:
    stream.expect("DROP")
    stream.expect("TABLE")
    table_name = stream.pop()
    _assert_consumed(stream)
    return DropTableStmt(table_name=table_name)


def _parse_alter(
    stream: TokenStream,
) -> AlterTableRenameStmt | AlterTableRenameColumnStmt | AlterTableAddColumnStmt | AlterTableRemoveColumnStmt:
    stream.expect("ALTER")
    stream.expect("TABLE")
    table_name = stream.pop()

    if stream.consume("RENAME"):
        if stream.consume("TO"):
            new_name = stream.pop()
            _assert_consumed(stream)
            return AlterTableRenameStmt(table_name=table_name, new_table_name=new_name)

        stream.expect("COLUMN")
        old_col = stream.pop()
        stream.expect("TO")
        new_col = stream.pop()
        _assert_consumed(stream)
        return AlterTableRenameColumnStmt(
            table_name=table_name,
            old_column_name=old_col,
            new_column_name=new_col,
        )

    if stream.consume("ADD"):
        stream.expect("COLUMN")
        col_name = stream.pop()
        col_type = stream.pop().upper()
        primary_key = False
        not_null = False
        if stream.consume("PRIMARY"):
            stream.expect("KEY")
            primary_key = True
            not_null = True
        if stream.consume("NOT"):
            stream.expect("NULL")
            not_null = True
        _assert_consumed(stream)
        return AlterTableAddColumnStmt(
            table_name=table_name,
            column=ColumnDef(
                name=col_name,
                data_type=col_type,
                primary_key=primary_key,
                not_null=not_null,
            ),
        )

    if stream.consume("REMOVE"):
        stream.expect("COLUMN")
        column_name = stream.pop()
        _assert_consumed(stream)
        return AlterTableRemoveColumnStmt(table_name=table_name, column_name=column_name)

    raise ParseError("Unsupported ALTER TABLE syntax")


def _parse_where(stream: TokenStream) -> WhereClause | None:
    if not stream.consume("WHERE"):
        return None

    predicates: List[Tuple[str, str, Any]] = []
    while True:
        col = stream.pop()
        op = stream.pop()
        if op not in {"=", "!=", "<", "<=", ">", ">="}:
            raise ParseError(f"Unsupported operator: {op}")
        value = _parse_literal(stream.pop())
        predicates.append((col, op, value))
        if stream.consume("AND"):
            continue
        break

    return WhereClause(predicates=predicates)


def _parse_literal(token: str) -> Any:
    upper = token.upper()
    if upper == "NULL":
        return None
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1]
    if re.fullmatch(r"\d+\.\d+", token):
        return float(token)
    if re.fullmatch(r"\d+", token):
        return int(token)
    # Unquoted identifiers as literals are intentionally supported to keep UPDATE concise.
    return token


def _assert_consumed(stream: TokenStream) -> None:
    if stream.peek() is not None:
        raise ParseError(f"Unexpected token: {stream.peek()}")

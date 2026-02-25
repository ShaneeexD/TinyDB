from __future__ import annotations

import re
from typing import Any, List, Sequence, Tuple

from .ast_nodes import (
    AlterTableAddColumnStmt,
    AlterTableRemoveColumnStmt,
    AlterTableRenameColumnStmt,
    AlterTableRenameStmt,
    BeginStmt,
    ColumnDef,
    CommitStmt,
    CreateIndexStmt,
    CreateTableStmt,
    DeleteStmt,
    DescribeTableStmt,
    DropIndexStmt,
    DropTableStmt,
    ExplainStmt,
    InsertStmt,
    JoinClause,
    RollbackStmt,
    SelectStmt,
    ShowIndexesStmt,
    ShowTablesStmt,
    Statement,
    UpdateStmt,
    WhereClause,
)

_TOKEN_RE = re.compile(
    r"\s*(=>|<=|>=|!=|[(),=*<>.]|\bAND\b|\bOR\b|\bIN\b|\bIS\b|\bLIKE\b|\bJOIN\b|\bLEFT\b|\bON\b|\bINDEX\b|\bASC\b|\bDESC\b|\bLIMIT\b|\bORDER\b|\bGROUP\b|\bBY\b|\bWHERE\b|\bFROM\b|\bVALUES\b|\bINTO\b|\bTABLE\b|\bCREATE\b|\bINSERT\b|\bSELECT\b|\bUPDATE\b|\bDELETE\b|\bDROP\b|\bSET\b|\bALTER\b|\bRENAME\b|\bADD\b|\bREMOVE\b|\bCOLUMN\b|\bTO\b|\bAS\b|\bPRIMARY\b|\bKEY\b|\bNOT\b|\bNULL\b|\bUNIQUE\b|\bDEFAULT\b|\bFOREIGN\b|\bREFERENCES\b|\bBEGIN\b|\bCOMMIT\b|\bROLLBACK\b|\bSHOW\b|\bDESCRIBE\b|\bEXPLAIN\b|\*|\bTRUE\b|\bFALSE\b|\bNULL\b|'(?:''|[^'])*'|\d+\.\d+|\d+|[A-Za-z_][A-Za-z0-9_]*)",
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


def _parse_identifier(stream: TokenStream) -> str:
    ident = stream.pop()
    if stream.consume("."):
        ident = f"{ident}.{stream.pop()}"
    return ident


def _parse_select_expression(stream: TokenStream) -> str:
    token = stream.pop()
    if stream.peek() != "(":
        if stream.consume("."):
            token = f"{token}.{stream.pop()}"
        return token

    expr_parts = [token, stream.pop()]
    depth = 1
    while depth > 0:
        part = stream.pop()
        if part == "(":
            depth += 1
        elif part == ")":
            depth -= 1
        expr_parts.append(part)
    return "".join(expr_parts)


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
    if keyword == "BEGIN":
        stream.pop()
        _assert_consumed(stream)
        return BeginStmt()
    if keyword == "COMMIT":
        stream.pop()
        _assert_consumed(stream)
        return CommitStmt()
    if keyword == "ROLLBACK":
        stream.pop()
        _assert_consumed(stream)
        return RollbackStmt()
    if keyword == "SHOW":
        stream.pop()
        if stream.consume("TABLES"):
            _assert_consumed(stream)
            return ShowTablesStmt()
        if stream.consume("INDEXES"):
            table_name = stream.pop() if stream.peek() is not None else None
            _assert_consumed(stream)
            return ShowIndexesStmt(table_name=table_name)
        raise ParseError("Expected TABLES or INDEXES after SHOW")
    if keyword == "EXPLAIN":
        stream.pop()
        rest = " ".join(stream.tokens[stream.pos :])
        if not rest.strip():
            raise ParseError("EXPLAIN requires a statement")
        return ExplainStmt(statement=parse(rest))
    if keyword == "DESCRIBE":
        stream.pop()
        table_name = stream.pop()
        _assert_consumed(stream)
        return DescribeTableStmt(table_name=table_name)
    raise ParseError(f"Unsupported command: {keyword}")


def _parse_create(stream: TokenStream) -> CreateTableStmt | CreateIndexStmt:
    stream.expect("CREATE")
    if stream.consume("INDEX"):
        index_name = stream.pop()
        stream.expect("ON")
        table_name = stream.pop()
        stream.expect("(")
        column_name = stream.pop()
        stream.expect(")")
        _assert_consumed(stream)
        return CreateIndexStmt(index_name=index_name, table_name=table_name, column_name=column_name)

    stream.expect("TABLE")
    table_name = stream.pop()
    stream.expect("(")

    columns: List[ColumnDef] = []
    foreign_keys: List[Tuple[str, str, str]] = []
    while True:
        if stream.consume("FOREIGN"):
            stream.expect("KEY")
            stream.expect("(")
            local_column = stream.pop()
            stream.expect(")")
            stream.expect("REFERENCES")
            ref_table = stream.pop()
            stream.expect("(")
            ref_column = stream.pop()
            stream.expect(")")
            foreign_keys.append((local_column, ref_table, ref_column))
        else:
            col_name = stream.pop()
            col_type = stream.pop().upper()
            primary_key = False
            not_null = False
            unique = False
            default_value: Any = None
            while True:
                if stream.consume("PRIMARY"):
                    stream.expect("KEY")
                    primary_key = True
                    not_null = True
                    continue
                if stream.consume("NOT"):
                    stream.expect("NULL")
                    not_null = True
                    continue
                if stream.consume("UNIQUE"):
                    unique = True
                    continue
                if stream.consume("DEFAULT"):
                    default_value = _parse_literal(stream.pop())
                    continue
                break
            columns.append(
                ColumnDef(
                    name=col_name,
                    data_type=col_type,
                    primary_key=primary_key,
                    not_null=not_null,
                    unique=unique,
                    default_value=default_value,
                )
            )

        if stream.consume(","):
            continue
        stream.expect(")")
        break

    _assert_consumed(stream)
    return CreateTableStmt(table_name=table_name, columns=columns, foreign_keys=foreign_keys)


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
            expr = _parse_select_expression(stream)
            if stream.consume("AS"):
                alias = stream.pop()
                expr = f"{expr} AS {alias}"
            columns.append(expr)
            if stream.consume(","):
                continue
            break

    stream.expect("FROM")
    table_name = _parse_identifier(stream)

    joins: List[JoinClause] = []
    while True:
        join_type = "INNER"
        if stream.consume("LEFT"):
            join_type = "LEFT"
            stream.expect("JOIN")
        elif stream.consume("JOIN"):
            join_type = "INNER"
        else:
            break

        join_table = _parse_identifier(stream)
        stream.expect("ON")
        join_left_column = _parse_identifier(stream)
        stream.expect("=")
        join_right_column = _parse_identifier(stream)
        joins.append(
            JoinClause(
                join_type=join_type,
                table_name=join_table,
                left_column=join_left_column,
                right_column=join_right_column,
            )
        )

    where = _parse_where(stream)

    group_by: List[str] | None = None
    if stream.consume("GROUP"):
        stream.expect("BY")
        group_by = []
        while True:
            group_by.append(_parse_identifier(stream))
            if stream.consume(","):
                continue
            break

    order_by = None
    if stream.consume("ORDER"):
        stream.expect("BY")
        col = _parse_identifier(stream)
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
        join_type=joins[0].join_type if joins else "INNER",
        join_table=joins[0].table_name if joins else None,
        join_left_column=joins[0].left_column if joins else None,
        join_right_column=joins[0].right_column if joins else None,
        joins=joins or None,
        where=where,
        group_by=group_by,
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
    if stream.consume("INDEX"):
        index_name = stream.pop()
        _assert_consumed(stream)
        return DropIndexStmt(index_name=index_name)
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
        unique = False
        default_value: Any = None
        while True:
            if stream.consume("PRIMARY"):
                stream.expect("KEY")
                primary_key = True
                not_null = True
                continue
            if stream.consume("NOT"):
                stream.expect("NULL")
                not_null = True
                continue
            if stream.consume("UNIQUE"):
                unique = True
                continue
            if stream.consume("DEFAULT"):
                default_value = _parse_literal(stream.pop())
                continue
            break
        _assert_consumed(stream)
        return AlterTableAddColumnStmt(
            table_name=table_name,
            column=ColumnDef(
                name=col_name,
                data_type=col_type,
                primary_key=primary_key,
                not_null=not_null,
                unique=unique,
                default_value=default_value,
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

    groups: List[List[Tuple[str, str, Any]]] = []
    current_group: List[Tuple[str, str, Any]] = []
    while True:
        col = _parse_identifier(stream)
        op = stream.pop().upper()

        if op == "IS":
            if stream.consume("NOT"):
                stream.expect("NULL")
                current_group.append((col, "IS NOT NULL", None))
            else:
                stream.expect("NULL")
                current_group.append((col, "IS NULL", None))
        elif op == "IN":
            stream.expect("(")
            values: List[Any] = []
            while True:
                values.append(_parse_literal(stream.pop()))
                if stream.consume(","):
                    continue
                stream.expect(")")
                break
            current_group.append((col, "IN", values))
        elif op == "NOT":
            stream.expect("IN")
            stream.expect("(")
            values = []
            while True:
                values.append(_parse_literal(stream.pop()))
                if stream.consume(","):
                    continue
                stream.expect(")")
                break
            current_group.append((col, "NOT IN", values))
        elif op == "LIKE":
            value = _parse_literal(stream.pop())
            current_group.append((col, "LIKE", value))
        else:
            if op not in {"=", "!=", "<", "<=", ">", ">="}:
                raise ParseError(f"Unsupported operator: {op}")
            value = _parse_literal(stream.pop())
            current_group.append((col, op, value))

        if stream.consume("AND"):
            continue
        if stream.consume("OR"):
            groups.append(current_group)
            current_group = []
            continue
        break

    groups.append(current_group)
    return WhereClause(groups=groups)


def _parse_literal(token: str) -> Any:
    upper = token.upper()
    if upper == "NULL":
        return None
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1].replace("''", "'")
    if re.fullmatch(r"\d+\.\d+", token):
        return float(token)
    if re.fullmatch(r"\d+", token):
        return int(token)
    # Unquoted identifiers as literals are intentionally supported to keep UPDATE concise.
    return token


def _assert_consumed(stream: TokenStream) -> None:
    if stream.peek() is not None:
        raise ParseError(f"Unexpected token: {stream.peek()}")

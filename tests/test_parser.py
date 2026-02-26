import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine.parser import ParseError, parse


def test_parse_select_distinct():
    stmt = parse("SELECT DISTINCT name FROM users")
    assert stmt.distinct is True
    assert stmt.columns == ["name"]
    assert stmt.table_name == "users"


def test_parse_create_table_check_constraints():
    stmt = parse(
        "CREATE TABLE users ("
        "id INTEGER PRIMARY KEY, "
        "age INTEGER CHECK (age >= 0), "
        "name TEXT, "
        "CHECK (age >= 0)"
        ")"
    )
    assert stmt.check_exprs == ("age >= 0",) or stmt.check_exprs == ["age >= 0"]
    age_col = next(col for col in stmt.columns if col.name == "age")
    assert age_col.check_exprs == ["age >= 0"]


def test_parse_select_group_by_having():
    stmt = parse("SELECT player, COUNT(*) AS total FROM games GROUP BY player HAVING total >= 2")
    assert stmt.group_by == ["player"]
    assert stmt.having is not None
    assert stmt.having.groups == [[("total", ">=", 2)]]


def test_parse_insert_or_replace():
    stmt = parse("INSERT OR REPLACE INTO users (id, name) VALUES (1, 'Alice')")
    assert stmt.or_replace is True
    assert stmt.table_name == "users"


def test_parse_create_index_multiple_columns():
    stmt = parse("CREATE INDEX idx_users_name_region ON users(name, region)")
    assert stmt.index_name == "idx_users_name_region"
    assert stmt.table_name == "users"
    assert list(stmt.column_names) == ["name", "region"]


def test_parse_create_table_check_expression_with_logic_and_arithmetic():
    stmt = parse(
        "CREATE TABLE users ("
        "id INTEGER PRIMARY KEY, "
        "wins INTEGER, losses INTEGER, games INTEGER, "
        "CHECK ((wins + losses = games) AND games >= 0)"
        ")"
    )
    assert len(stmt.check_exprs) == 1
    expr = str(stmt.check_exprs[0])
    assert "wins + losses = games" in expr
    assert "AND games >= 0" in expr


def test_parse_error_includes_line_and_column():
    with pytest.raises(ParseError) as exc_info:
        parse("SELECT id\nFROM users\nWHERE name = @bad")

    message = str(exc_info.value)
    assert "line" in message
    assert "col" in message
    assert "near" in message

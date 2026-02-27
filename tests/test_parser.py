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


def test_parse_create_table_foreign_key_on_delete_cascade():
    stmt = parse(
        "CREATE TABLE games ("
        "id INTEGER PRIMARY KEY, "
        "user_id INTEGER, "
        "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE"
        ")"
    )
    assert list(stmt.foreign_keys) == [("user_id", "users", "id", "CASCADE")]


def test_parse_create_table_foreign_key_on_delete_invalid_action():
    with pytest.raises(ParseError, match="Unsupported ON DELETE action"):
        parse(
            "CREATE TABLE games ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER, "
            "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET"
            ")"
        )


def test_parse_select_group_by_having():
    stmt = parse("SELECT player, COUNT(*) AS total FROM games GROUP BY player HAVING total >= 2")
    assert stmt.group_by == ["player"]
    assert stmt.having is not None
    assert stmt.having.groups == [[("total", ">=", 2)]]


def test_parse_select_count_case_when_expression():
    stmt = parse("SELECT COUNT(CASE WHEN win = 1 THEN 1 END) AS num_wins FROM bets")
    assert stmt.columns == ["COUNT(CASE WHEN win = 1 THEN 1 END) AS num_wins"]


def test_parse_having_scalar_subquery_comparison():
    stmt = parse("SELECT COUNT(*) AS c FROM bets HAVING c = (SELECT COUNT(*) FROM bets)")
    assert stmt.having is not None
    assert stmt.having.groups == [[("c", "=_SUBQUERY", "SELECT COUNT(*) FROM bets")]]


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


def test_parse_create_table_composite_primary_key():
    stmt = parse("CREATE TABLE memberships (user_id INTEGER, org_id INTEGER, role TEXT, PRIMARY KEY (user_id, org_id))")
    assert list(stmt.primary_key_columns) == ["user_id", "org_id"]


def test_parse_where_in_select_subquery():
    stmt = parse("SELECT id FROM users WHERE id IN (SELECT user_id FROM memberships)")
    assert stmt.where is not None
    assert stmt.where.groups == [[("id", "IN_SUBQUERY", "SELECT user_id FROM memberships")]]


def test_parse_where_not_in_select_subquery():
    stmt = parse("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM memberships)")
    assert stmt.where is not None
    assert stmt.where.groups == [[("id", "NOT IN_SUBQUERY", "SELECT user_id FROM memberships")]]


def test_parse_select_with_table_aliases():
    stmt = parse(
        "SELECT c.username, c.amount, r.id AS round_id "
        "FROM coinflip_bets c "
        "JOIN coinflip_rounds r ON c.round_id = r.id "
        "WHERE c.win = TRUE "
        "ORDER BY c.resolved_at DESC "
        "LIMIT 10"
    )
    assert stmt.columns == [
        "coinflip_bets.username",
        "coinflip_bets.amount",
        "coinflip_rounds.id AS round_id",
    ]
    assert stmt.join_table == "coinflip_rounds"
    assert stmt.join_left_column == "coinflip_bets.round_id"
    assert stmt.join_right_column == "coinflip_rounds.id"
    assert stmt.where is not None
    assert stmt.where.groups == [[("coinflip_bets.win", "=", True)]]
    assert stmt.order_by == ("coinflip_bets.resolved_at", "DESC")
    assert stmt.limit == 10


def test_parse_error_includes_line_and_column():
    with pytest.raises(ParseError) as exc_info:
        parse("SELECT id\nFROM users\nWHERE name = @bad")

    message = str(exc_info.value)
    assert "line" in message
    assert "col" in message
    assert "near" in message


def test_parse_reindex_table():
    stmt = parse("REINDEX users")
    assert stmt.table_name == "users"


def test_parse_where_between_predicate():
    stmt = parse("SELECT id FROM users WHERE id BETWEEN 1 AND 3")
    assert stmt.where is not None
    assert stmt.where.groups == [[("id", "BETWEEN", (1, 3))]]

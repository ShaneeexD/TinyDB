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


def test_parse_select_group_by_having():
    stmt = parse("SELECT player, COUNT(*) AS total FROM games GROUP BY player HAVING total >= 2")
    assert stmt.group_by == ["player"]
    assert stmt.having is not None
    assert stmt.having.groups == [[("total", ">=", 2)]]


def test_parse_error_includes_line_and_column():
    with pytest.raises(ParseError) as exc_info:
        parse("SELECT id\nFROM users\nWHERE name = @bad")

    message = str(exc_info.value)
    assert "line" in message
    assert "col" in message
    assert "near" in message

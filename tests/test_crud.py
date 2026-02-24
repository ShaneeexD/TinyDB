import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_basic_crud(tmp_path):
    db_path = tmp_path / "crud.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)"
        ) == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)") == "OK"

        rows = db.execute("SELECT * FROM users ORDER BY id ASC")
        assert rows == [
            {"id": 1, "name": "Alice", "score": 9.5, "active": True},
            {"id": 2, "name": "Bob", "score": 7.0, "active": False},
        ]

        affected = db.execute("UPDATE users SET score = 8.2 WHERE id = 2")
        assert affected == 1

        rows = db.execute("SELECT name, score FROM users WHERE id = 2")
        assert rows == [{"name": "Bob", "score": 8.2}]

        deleted = db.execute("DELETE FROM users WHERE id = 1")
        assert deleted == 1

        rows = db.execute("SELECT * FROM users ORDER BY id ASC")
        assert rows == [{"id": 2, "name": "Bob", "score": 8.2, "active": False}]
    finally:
        db.close()


def test_select_left_join_support(tmp_path):
    db_path = tmp_path / "crud_left_join.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER, coin_side TEXT)") == "OK"

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO games VALUES (10, 1, 'heads')")

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users LEFT JOIN games ON users.id = games.user_id "
            "ORDER BY users.id ASC"
        )
        assert rows == [
            {"users.name": "Alice", "games.coin_side": "heads"},
            {"users.name": "Bob", "games.coin_side": None},
        ]
    finally:
        db.close()


def test_group_by_and_aggregates(tmp_path):
    db_path = tmp_path / "crud_group_by.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, coin_side TEXT, amount REAL)") == "OK"
        db.execute("INSERT INTO games VALUES (1, 'heads', 5.0)")
        db.execute("INSERT INTO games VALUES (2, 'heads', 7.5)")
        db.execute("INSERT INTO games VALUES (3, 'tails', 2.5)")

        rows = db.execute("SELECT coin_side, COUNT(*), SUM(amount) FROM games GROUP BY coin_side ORDER BY coin_side ASC")
        assert rows == [
            {"coin_side": "heads", "COUNT(*)": 2, "SUM(amount)": 12.5},
            {"coin_side": "tails", "COUNT(*)": 1, "SUM(amount)": 2.5},
        ]
    finally:
        db.close()


def test_select_inner_join_basic(tmp_path):
    db_path = tmp_path / "crud_join_basic.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, coin_side TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO games VALUES (10, 1, 'heads')") == "OK"
        assert db.execute("INSERT INTO games VALUES (11, 2, 'tails')") == "OK"

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users JOIN games ON users.id = games.user_id "
            "ORDER BY users.name ASC"
        )
        assert rows == [
            {"users.name": "Alice", "games.coin_side": "heads"},
            {"users.name": "Bob", "games.coin_side": "tails"},
        ]
    finally:
        db.close()


def test_select_inner_join_with_where_and_limit(tmp_path):
    db_path = tmp_path / "crud_join_where_limit.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, coin_side TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO games VALUES (10, 1, 'heads')") == "OK"
        assert db.execute("INSERT INTO games VALUES (11, 1, 'tails')") == "OK"
        assert db.execute("INSERT INTO games VALUES (12, 2, 'heads')") == "OK"

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users JOIN games ON users.id = games.user_id "
            "WHERE users.name = 'Alice' "
            "ORDER BY games.id DESC LIMIT 1"
        )
        assert rows == [{"users.name": "Alice", "games.coin_side": "tails"}]
    finally:
        db.close()


def test_where_like_support(tmp_path):
    db_path = tmp_path / "crud_where_like.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Alicia')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Bob')") == "OK"

        rows = db.execute("SELECT id FROM users WHERE name LIKE 'Ali%' ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 2}]

        rows = db.execute("SELECT id FROM users WHERE name LIKE 'A_i_e' ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_where_not_in_support(tmp_path):
    db_path = tmp_path / "crud_where_not_in.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Cara')") == "OK"

        rows = db.execute("SELECT id FROM users WHERE id NOT IN (2, 3) ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_where_is_null_and_is_not_null(tmp_path):
    db_path = tmp_path / "crud_where_is_null.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, NULL, NULL)") == "OK"

        rows = db.execute("SELECT id FROM users WHERE name IS NULL ORDER BY id ASC")
        assert rows == [{"id": 2}]

        rows = db.execute("SELECT id FROM users WHERE score IS NOT NULL ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_string_literal_escaped_single_quote(tmp_path):
    db_path = tmp_path / "crud_escaped_quote.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'O''Brien')") == "OK"

        rows = db.execute("SELECT name FROM users WHERE id = 1")
        assert rows == [{"name": "O'Brien"}]
    finally:
        db.close()


def test_where_or_and_in_support(tmp_path):
    db_path = tmp_path / "crud_where_or_in.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)"
        ) == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Cara', 8.1, TRUE)") == "OK"

        rows = db.execute("SELECT id FROM users WHERE id = 1 OR id = 3 ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 3}]

        rows = db.execute("SELECT id FROM users WHERE id IN (2, 3) ORDER BY id ASC")
        assert rows == [{"id": 2}, {"id": 3}]

        rows = db.execute("SELECT id FROM users WHERE active = TRUE AND id IN (1, 3) ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 3}]
    finally:
        db.close()


def test_timestamp_type_round_trip(tmp_path):
    db_path = tmp_path / "crud_timestamp.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL, note TEXT)"
        ) == "OK"

        assert db.execute(
            "INSERT INTO events VALUES (1, '2023-04-01 12:34:56', 'boot')"
        ) == "OK"

        rows = db.execute("SELECT created_at, note FROM events WHERE id = 1")
        assert rows == [{"created_at": "2023-04-01 12:34:56", "note": "boot"}]
    finally:
        db.close()


def test_multi_row_insert(tmp_path):
    db_path = tmp_path / "crud_multi_insert.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE games (id INTEGER PRIMARY KEY, coin_side TEXT NOT NULL, bet_amount REAL, outcome BOOLEAN)"
        ) == "OK"

        assert db.execute(
            "INSERT INTO games (id, coin_side, bet_amount, outcome) VALUES "
            "(1, 'heads', 5.0, TRUE), "
            "(2, 'tails', 3.5, FALSE), "
            "(3, 'heads', 7.2, TRUE)"
        ) == "OK"

        rows = db.execute("SELECT * FROM games ORDER BY id ASC")
        assert rows == [
            {"id": 1, "coin_side": "heads", "bet_amount": 5.0, "outcome": True},
            {"id": 2, "coin_side": "tails", "bet_amount": 3.5, "outcome": False},
            {"id": 3, "coin_side": "heads", "bet_amount": 7.2, "outcome": True},
        ]
    finally:
        db.close()

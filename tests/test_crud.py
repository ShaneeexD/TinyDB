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

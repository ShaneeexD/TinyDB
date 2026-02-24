import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_explicit_transaction_commit(tmp_path):
    db = TinyDB(str(tmp_path / "tx_commit.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        assert db.execute("BEGIN") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("COMMIT") == "OK"
        assert db.execute("SELECT * FROM users") == [{"id": 1, "name": "Alice"}]
    finally:
        db.close()


def test_execute_with_params_escapes_single_quotes(tmp_path):
    db = TinyDB(str(tmp_path / "params_quotes.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        assert db.execute(
            "INSERT INTO users VALUES (?, ?)",
            params=[1, "O'Brien"],
        ) == "OK"

        rows = db.execute("SELECT name FROM users WHERE id = ?", params=[1])
        assert rows == [{"name": "O'Brien"}]
    finally:
        db.close()


def test_explicit_transaction_rollback(tmp_path):
    db = TinyDB(str(tmp_path / "tx_rollback.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        assert db.execute("BEGIN") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("ROLLBACK") == "OK"
        assert db.execute("SELECT * FROM users") == []
    finally:
        db.close()


def test_explicit_transaction_errors(tmp_path):
    db = TinyDB(str(tmp_path / "tx_errors.db"))
    try:
        with pytest.raises(ValueError, match="No active transaction to COMMIT"):
            db.execute("COMMIT")
        with pytest.raises(ValueError, match="No active transaction to ROLLBACK"):
            db.execute("ROLLBACK")

        assert db.execute("BEGIN") == "OK"
        with pytest.raises(ValueError, match="Transaction already active"):
            db.execute("BEGIN")
        assert db.execute("ROLLBACK") == "OK"
    finally:
        db.close()


def test_execute_with_positional_params(tmp_path):
    db = TinyDB(str(tmp_path / "params.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN)")
        assert db.execute(
            "INSERT INTO users VALUES (?, ?, ?)",
            params=[1, "Alice", True],
        ) == "OK"

        rows = db.execute("SELECT * FROM users WHERE id = ?", params=[1])
        assert rows == [{"id": 1, "name": "Alice", "active": True}]
    finally:
        db.close()


def test_execute_params_placeholder_count_validation(tmp_path):
    db = TinyDB(str(tmp_path / "params_count.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        with pytest.raises(ValueError, match="Not enough parameters"):
            db.execute("INSERT INTO users VALUES (?, ?)", params=[1])
        with pytest.raises(ValueError, match="Too many parameters"):
            db.execute("INSERT INTO users VALUES (?, ?)", params=[1, "Alice", "extra"])
    finally:
        db.close()

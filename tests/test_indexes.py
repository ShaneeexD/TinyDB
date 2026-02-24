import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_create_index_on_unique_column_and_select(tmp_path):
    db = TinyDB(str(tmp_path / "index_select.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
        db.execute("INSERT INTO users VALUES (1, 'a@example.com', 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'b@example.com', 'Bob')")

        assert db.execute("CREATE INDEX idx_users_email ON users(email)") == "OK"

        rows = db.execute("SELECT id, name FROM users WHERE email = 'b@example.com'")
        assert rows == [{"id": 2, "name": "Bob"}]
    finally:
        db.close()


def test_index_kept_consistent_on_update_and_delete(tmp_path):
    db = TinyDB(str(tmp_path / "index_mutations.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        db.execute("INSERT INTO users VALUES (1, 'old@example.com')")
        db.execute("CREATE INDEX idx_users_email ON users(email)")

        assert db.execute("UPDATE users SET email = 'new@example.com' WHERE id = 1") == 1
        rows = db.execute("SELECT id FROM users WHERE email = 'new@example.com'")
        assert rows == [{"id": 1}]

        assert db.execute("DELETE FROM users WHERE id = 1") == 1
        rows = db.execute("SELECT id FROM users WHERE email = 'new@example.com'")
        assert rows == []
    finally:
        db.close()


def test_create_index_rejects_non_unique_column(tmp_path):
    db = TinyDB(str(tmp_path / "index_non_unique.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        with pytest.raises(ValueError, match="UNIQUE columns"):
            db.execute("CREATE INDEX idx_users_name ON users(name)")
    finally:
        db.close()

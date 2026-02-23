import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from tinydb_engine import TinyDB


def test_duplicate_primary_key_rejected(tmp_path):
    db = TinyDB(str(tmp_path / "pk.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.execute("INSERT INTO users VALUES (1, 'Bob')")
    finally:
        db.close()


def test_foreign_key_references_enforced_on_insert(tmp_path):
    db = TinyDB(str(tmp_path / "fk_insert.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute(
            "CREATE TABLE games ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER, "
            "coin_side TEXT, "
            "FOREIGN KEY (user_id) REFERENCES users(id)"
            ")"
        )

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO games VALUES (10, 1, 'heads')")

        with pytest.raises(ValueError, match="FOREIGN KEY constraint failed"):
            db.execute("INSERT INTO games VALUES (11, 999, 'tails')")
    finally:
        db.close()


def test_foreign_key_references_enforced_on_delete_parent(tmp_path):
    db = TinyDB(str(tmp_path / "fk_delete.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute(
            "CREATE TABLE games ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER, "
            "FOREIGN KEY (user_id) REFERENCES users(id)"
            ")"
        )

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO games VALUES (10, 1)")

        with pytest.raises(ValueError, match="FOREIGN KEY constraint failed"):
            db.execute("DELETE FROM users WHERE id = 1")
    finally:
        db.close()


def test_not_null_rejected(tmp_path):
    db = TinyDB(str(tmp_path / "notnull.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        with pytest.raises(ValueError, match="cannot be NULL"):
            db.execute("INSERT INTO users VALUES (2, NULL)")
    finally:
        db.close()

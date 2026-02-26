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


def test_check_constraint_with_is_null_and_is_not_null(tmp_path):
    db = TinyDB(str(tmp_path / "check_is_null.db"))
    try:
        db.execute(
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "nickname TEXT, "
            "CHECK (nickname IS NULL OR nickname IS NOT NULL)"
            ")"
        )
        db.execute("INSERT INTO users VALUES (1, NULL)")
        db.execute("INSERT INTO users VALUES (2, 'alice')")
        rows = db.execute("SELECT id FROM users ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 2}]
    finally:
        db.close()


def test_check_constraint_rejects_when_is_null_fails(tmp_path):
    db = TinyDB(str(tmp_path / "check_is_null_fail.db"))
    try:
        db.execute(
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "nickname TEXT, "
            "CHECK (nickname IS NULL)"
            ")"
        )
        db.execute("INSERT INTO users VALUES (1, NULL)")
        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users VALUES (2, 'alice')")
    finally:
        db.close()


def test_check_constraint_with_and_or(tmp_path):
    db = TinyDB(str(tmp_path / "check_logic.db"))
    try:
        db.execute(
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "age INTEGER, "
            "score INTEGER, "
            "CHECK ((age >= 18 AND score >= 50) OR age < 18)"
            ")"
        )
        db.execute("INSERT INTO users VALUES (1, 17, 10)")
        db.execute("INSERT INTO users VALUES (2, 18, 60)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users VALUES (3, 19, 40)")
    finally:
        db.close()


def test_check_constraint_with_arithmetic(tmp_path):
    db = TinyDB(str(tmp_path / "check_arith.db"))
    try:
        db.execute(
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "wins INTEGER, "
            "losses INTEGER, "
            "games INTEGER, "
            "CHECK (wins + losses = games)"
            ")"
        )
        db.execute("INSERT INTO users VALUES (1, 3, 2, 5)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users VALUES (2, 3, 2, 4)")
    finally:
        db.close()


def test_check_constraint_enforced_on_insert_and_update(tmp_path):
    db = TinyDB(str(tmp_path / "check.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0))")
        db.execute("INSERT INTO users VALUES (1, 10)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users VALUES (2, -1)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("UPDATE users SET age = -5 WHERE id = 1")
    finally:
        db.close()


def test_table_level_check_constraint(tmp_path):
    db = TinyDB(str(tmp_path / "check_table.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, min_age INTEGER, max_age INTEGER, CHECK (min_age <= max_age))")
        db.execute("INSERT INTO users VALUES (1, 10, 20)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users VALUES (2, 30, 20)")
    finally:
        db.close()


def test_alter_add_column_check_constraint(tmp_path):
    db = TinyDB(str(tmp_path / "check_alter.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        db.execute("ALTER TABLE users ADD COLUMN score INTEGER CHECK (score >= 0)")
        db.execute("INSERT INTO users (id, score) VALUES (1, 5)")

        with pytest.raises(ValueError, match="CHECK constraint failed"):
            db.execute("INSERT INTO users (id, score) VALUES (2, -1)")
    finally:
        db.close()


def test_unique_constraint_enforced_on_insert_and_update(tmp_path):
    db = TinyDB(str(tmp_path / "unique.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        db.execute("INSERT INTO users VALUES (1, 'alice@example.com')")
        db.execute("INSERT INTO users VALUES (2, 'bob@example.com')")

        with pytest.raises(ValueError, match="UNIQUE constraint failed"):
            db.execute("INSERT INTO users VALUES (3, 'alice@example.com')")

        with pytest.raises(ValueError, match="UNIQUE constraint failed"):
            db.execute("UPDATE users SET email = 'alice@example.com' WHERE id = 2")
    finally:
        db.close()


def test_default_value_on_create_and_alter_add_column(tmp_path):
    db = TinyDB(str(tmp_path / "defaults.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, role TEXT DEFAULT 'player', active BOOLEAN DEFAULT TRUE)")
        db.execute("INSERT INTO users (id) VALUES (1)")
        rows = db.execute("SELECT role, active FROM users WHERE id = 1")
        assert rows == [{"role": "player", "active": True}]

        db.execute("ALTER TABLE users ADD COLUMN region TEXT DEFAULT 'NA'")
        rows = db.execute("SELECT region FROM users WHERE id = 1")
        assert rows == [{"region": "NA"}]

        db.execute("INSERT INTO users (id, role) VALUES (2, 'admin')")
        rows = db.execute("SELECT role, active, region FROM users WHERE id = 2")
        assert rows == [{"role": "admin", "active": True, "region": "NA"}]
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


def test_foreign_key_on_delete_cascade(tmp_path):
    db = TinyDB(str(tmp_path / "fk_cascade.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute(
            "CREATE TABLE games ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER, "
            "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE"
            ")"
        )

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO games VALUES (10, 1)")
        db.execute("INSERT INTO games VALUES (11, 1)")
        db.execute("INSERT INTO games VALUES (12, 2)")

        affected = db.execute("DELETE FROM users WHERE id = 1")
        assert affected == 1

        rows = db.execute("SELECT id, user_id FROM games ORDER BY id ASC")
        assert rows == [{"id": 12, "user_id": 2}]
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

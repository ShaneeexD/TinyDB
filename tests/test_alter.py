import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_alter_table_rename_and_select(tmp_path):
    db = TinyDB(str(tmp_path / "alter.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        assert db.execute("ALTER TABLE users RENAME TO members") == "OK"
        rows = db.execute("SELECT * FROM members")
        assert rows == [{"id": 1, "name": "Alice"}]
    finally:
        db.close()


def test_alter_table_rename_column(tmp_path):
    db = TinyDB(str(tmp_path / "alter_col.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        assert db.execute("ALTER TABLE users RENAME COLUMN name TO full_name") == "OK"
        rows = db.execute("SELECT full_name FROM users WHERE id = 1")
        assert rows == [{"full_name": "Alice"}]
    finally:
        db.close()


def test_alter_table_add_column(tmp_path):
    db = TinyDB(str(tmp_path / "alter_add.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        assert db.execute("ALTER TABLE users ADD COLUMN test INTEGER") == "OK"

        rows = db.execute("SELECT id, name, test FROM users WHERE id = 1")
        assert rows == [{"id": 1, "name": "Alice", "test": None}]

        db.execute("INSERT INTO users VALUES (2, 'Bob', 42)")
        rows = db.execute("SELECT test FROM users WHERE id = 2")
        assert rows == [{"test": 42}]
    finally:
        db.close()


def test_alter_table_add_column_rejects_not_null(tmp_path):
    db = TinyDB(str(tmp_path / "alter_add_not_null.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        try:
            db.execute("ALTER TABLE users ADD COLUMN test INTEGER NOT NULL")
            assert False, "Expected ALTER TABLE ADD COLUMN NOT NULL to fail"
        except ValueError as exc:
            assert "does not support NOT NULL" in str(exc)
    finally:
        db.close()


def test_alter_table_remove_column(tmp_path):
    db = TinyDB(str(tmp_path / "alter_remove.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("ALTER TABLE users ADD COLUMN test INTEGER")
        db.execute("UPDATE users SET test = 9 WHERE id = 1")

        assert db.execute("ALTER TABLE users REMOVE COLUMN test") == "OK"

        rows = db.execute("SELECT * FROM users WHERE id = 1")
        assert rows == [{"id": 1, "name": "Alice"}]
    finally:
        db.close()


def test_alter_table_remove_column_requires_last_column(tmp_path):
    db = TinyDB(str(tmp_path / "alter_remove_non_last.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("ALTER TABLE users ADD COLUMN test INTEGER")

        try:
            db.execute("ALTER TABLE users REMOVE COLUMN name")
            assert False, "Expected removing non-last column to fail"
        except ValueError as exc:
            assert "supports only the last column" in str(exc)
    finally:
        db.close()


def test_drop_table(tmp_path):
    db = TinyDB(str(tmp_path / "drop_table.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        assert db.execute("DROP TABLE users") == "OK"

        try:
            db.execute("SELECT * FROM users")
            assert False, "Expected dropped table select to fail"
        except ValueError as exc:
            assert "Unknown table" in str(exc)
    finally:
        db.close()

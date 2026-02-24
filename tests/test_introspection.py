import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_show_tables(tmp_path):
    db = TinyDB(str(tmp_path / "show_tables.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER)")

        rows = db.execute("SHOW TABLES")
        assert rows == [{"table_name": "games"}, {"table_name": "users"}]
    finally:
        db.close()


def test_describe_table(tmp_path):
    db = TinyDB(str(tmp_path / "describe_table.db"))
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

        rows = db.execute("DESCRIBE games")
        assert rows == [
            {"name": "id", "data_type": "INTEGER", "primary_key": True, "not_null": True, "foreign_key": None},
            {
                "name": "user_id",
                "data_type": "INTEGER",
                "primary_key": False,
                "not_null": False,
                "foreign_key": "users.id",
            },
            {"name": "coin_side", "data_type": "TEXT", "primary_key": False, "not_null": False, "foreign_key": None},
        ]
    finally:
        db.close()

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_where_order_limit(tmp_path):
    db = TinyDB(str(tmp_path / "select.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)")
        db.execute("INSERT INTO users VALUES (1, 'A', 3.0)")
        db.execute("INSERT INTO users VALUES (2, 'B', 7.5)")
        db.execute("INSERT INTO users VALUES (3, 'C', 5.5)")

        rows = db.execute(
            "SELECT id, score FROM users WHERE score >= 5.0 AND id != 2 ORDER BY score DESC LIMIT 1"
        )
        assert rows == [{"id": 3, "score": 5.5}]
    finally:
        db.close()

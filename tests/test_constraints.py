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


def test_not_null_rejected(tmp_path):
    db = TinyDB(str(tmp_path / "notnull.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        with pytest.raises(ValueError, match="cannot be NULL"):
            db.execute("INSERT INTO users VALUES (2, NULL)")
    finally:
        db.close()

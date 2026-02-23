import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_create_and_authenticate_user(tmp_path):
    db = TinyDB(str(tmp_path / "auth_api.db"))
    try:
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, password_hash TEXT NOT NULL)"
        )

        assert db.create_user("alice", "SuperSecret123") == "OK"
        assert db.authenticate_user("alice", "SuperSecret123") is True
        assert db.authenticate_user("alice", "wrong") is False
        assert db.authenticate_user("missing", "SuperSecret123") is False
    finally:
        db.close()


def test_create_user_rejects_invalid_username(tmp_path):
    db = TinyDB(str(tmp_path / "auth_api_invalid.db"))
    try:
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, password_hash TEXT NOT NULL)"
        )

        try:
            db.create_user("", "abc123")
            assert False, "Expected empty username to fail"
        except ValueError as exc:
            assert "Username cannot be empty" in str(exc)

        try:
            db.create_user("o'hara", "abc123")
            assert False, "Expected username with single quote to fail"
        except ValueError as exc:
            assert "single quotes" in str(exc)
    finally:
        db.close()

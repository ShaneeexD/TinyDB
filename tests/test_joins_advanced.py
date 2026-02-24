import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_multi_join_inner_chain(tmp_path):
    db = TinyDB(str(tmp_path / "join_chain.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT)")
        db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, team_id INTEGER, result TEXT)")

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO teams VALUES (10, 1, 'Ravens')")
        db.execute("INSERT INTO teams VALUES (11, 2, 'Wolves')")
        db.execute("INSERT INTO games VALUES (100, 10, 'W')")
        db.execute("INSERT INTO games VALUES (101, 11, 'L')")

        rows = db.execute(
            "SELECT users.name, teams.name, games.result "
            "FROM users "
            "JOIN teams ON users.id = teams.user_id "
            "JOIN games ON teams.id = games.team_id "
            "ORDER BY users.id ASC"
        )
        assert rows == [
            {"users.name": "Alice", "teams.name": "Ravens", "games.result": "W"},
            {"users.name": "Bob", "teams.name": "Wolves", "games.result": "L"},
        ]
    finally:
        db.close()


def test_multi_join_with_left_tail(tmp_path):
    db = TinyDB(str(tmp_path / "join_left_tail.db"))
    try:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        db.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT)")
        db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, team_id INTEGER, result TEXT)")

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO teams VALUES (10, 1, 'Ravens')")
        db.execute("INSERT INTO teams VALUES (11, 2, 'Wolves')")
        db.execute("INSERT INTO games VALUES (100, 10, 'W')")

        rows = db.execute(
            "SELECT users.name, teams.name, games.result "
            "FROM users "
            "JOIN teams ON users.id = teams.user_id "
            "LEFT JOIN games ON teams.id = games.team_id "
            "ORDER BY users.id ASC"
        )
        assert rows == [
            {"users.name": "Alice", "teams.name": "Ravens", "games.result": "W"},
            {"users.name": "Bob", "teams.name": "Wolves", "games.result": None},
        ]
    finally:
        db.close()

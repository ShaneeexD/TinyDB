import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_basic_crud(tmp_path):
    db_path = tmp_path / "crud.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)"
        ) == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)") == "OK"

        rows = db.execute("SELECT * FROM users ORDER BY id ASC")
        assert rows == [
            {"id": 1, "name": "Alice", "score": 9.5, "active": True},
            {"id": 2, "name": "Bob", "score": 7.0, "active": False},
        ]

        affected = db.execute("UPDATE users SET score = 8.2 WHERE id = 2")
        assert affected == 1

        rows = db.execute("SELECT name, score FROM users WHERE id = 2")
        assert rows == [{"name": "Bob", "score": 8.2}]

        deleted = db.execute("DELETE FROM users WHERE id = 1")
        assert deleted == 1

        rows = db.execute("SELECT * FROM users ORDER BY id ASC")
        assert rows == [{"id": 2, "name": "Bob", "score": 8.2, "active": False}]
    finally:
        db.close()


def test_where_in_select_subquery_support(tmp_path):
    db_path = tmp_path / "crud_where_in_subquery.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE memberships (user_id INTEGER PRIMARY KEY, org TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Cara')") == "OK"
        assert db.execute("INSERT INTO memberships VALUES (1, 'A')") == "OK"
        assert db.execute("INSERT INTO memberships VALUES (3, 'B')") == "OK"

        rows = db.execute("SELECT id FROM users WHERE id IN (SELECT user_id FROM memberships) ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 3}]

        rows = db.execute("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM memberships) ORDER BY id ASC")
        assert rows == [{"id": 2}]
    finally:
        db.close()


def test_composite_primary_key_support(tmp_path):
    db_path = tmp_path / "crud_composite_pk.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE memberships ("
            "user_id INTEGER, "
            "org_id INTEGER, "
            "role TEXT, "
            "PRIMARY KEY (user_id, org_id)"
            ")"
        ) == "OK"

        assert db.execute("INSERT INTO memberships VALUES (1, 10, 'member')") == "OK"
        assert db.execute("INSERT INTO memberships VALUES (1, 11, 'admin')") == "OK"

        rows = db.execute("SELECT role FROM memberships WHERE user_id = 1 AND org_id = 11")
        assert rows == [{"role": "admin"}]

        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.execute("INSERT INTO memberships VALUES (1, 10, 'owner')")
    finally:
        db.close()


def test_insert_or_replace_on_primary_key_conflict(tmp_path):
    db_path = tmp_path / "crud_insert_or_replace.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE INDEX idx_users_email ON users(email)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'a@x.com', 'Alice')") == "OK"
        assert db.execute("INSERT OR REPLACE INTO users VALUES (1, 'b@x.com', 'Alicia')") == "OK"

        rows = db.execute("SELECT id, email, name FROM users ORDER BY id ASC")
        assert rows == [{"id": 1, "email": "b@x.com", "name": "Alicia"}]

        rows = db.execute("SELECT id FROM users WHERE email = 'a@x.com'")
        assert rows == []
    finally:
        db.close()


def test_select_distinct_support(tmp_path):
    db_path = tmp_path / "crud_distinct.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Bob')") == "OK"

        rows = db.execute("SELECT DISTINCT name FROM users ORDER BY name ASC")
        assert rows == [{"name": "Alice"}, {"name": "Bob"}]
    finally:
        db.close()


def test_group_by_having_support(tmp_path):
    db_path = tmp_path / "crud_having.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, player TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO games VALUES (1, 'A')") == "OK"
        assert db.execute("INSERT INTO games VALUES (2, 'A')") == "OK"
        assert db.execute("INSERT INTO games VALUES (3, 'B')") == "OK"

        rows = db.execute(
            "SELECT player, COUNT(*) AS total "
            "FROM games "
            "GROUP BY player "
            "HAVING total >= 2 "
            "ORDER BY player ASC"
        )
        assert rows == [{"player": "A", "total": 2}]
    finally:
        db.close()


def test_create_table_if_not_exists(tmp_path):
    db_path = tmp_path / "crud_if_not_exists.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
    finally:
        db.close()


def test_autoincrement_integer_primary_key(tmp_path):
    db_path = tmp_path / "crud_autoinc.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users (name) VALUES ('Alice')") == "OK"
        assert db.execute("INSERT INTO users (name) VALUES ('Bob')") == "OK"
        assert db.execute("INSERT INTO users VALUES (NULL, 'Cara')") == "OK"

        rows = db.execute("SELECT id, name FROM users ORDER BY id ASC")
        assert rows == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Cara"},
        ]
    finally:
        db.close()


def test_select_as_alias_support(tmp_path):
    db_path = tmp_path / "crud_alias.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, points REAL NOT NULL)") == "OK"

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO scores VALUES (10, 1, 9.5)")
        db.execute("INSERT INTO scores VALUES (11, 1, 7.0)")
        db.execute("INSERT INTO scores VALUES (12, 2, 6.5)")

        rows = db.execute("SELECT id AS user_id, name AS username FROM users ORDER BY id ASC")
        assert rows == [{"user_id": 1, "username": "Alice"}, {"user_id": 2, "username": "Bob"}]

        rows = db.execute("SELECT user_id, SUM(points) AS total_points FROM scores GROUP BY user_id ORDER BY user_id ASC")
        assert rows == [{"user_id": 1, "total_points": 16.5}, {"user_id": 2, "total_points": 6.5}]

        rows = db.execute(
            "SELECT users.name AS player_name, scores.points AS score "
            "FROM users JOIN scores ON users.id = scores.user_id "
            "WHERE users.id = 1 "
            "ORDER BY scores.id ASC LIMIT 1"
        )
        assert rows == [{"player_name": "Alice", "score": 9.5}]
    finally:
        db.close()


def test_select_left_join_support(tmp_path):
    db_path = tmp_path / "crud_left_join.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER, coin_side TEXT)") == "OK"

        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")
        db.execute("INSERT INTO games VALUES (10, 1, 'heads')")

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users LEFT JOIN games ON users.id = games.user_id "
            "ORDER BY users.id ASC"
        )
        assert rows == [
            {"users.name": "Alice", "games.coin_side": "heads"},
            {"users.name": "Bob", "games.coin_side": None},
        ]
    finally:
        db.close()


def test_group_by_and_aggregates(tmp_path):
    db_path = tmp_path / "crud_group_by.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, coin_side TEXT, amount REAL)") == "OK"
        db.execute("INSERT INTO games VALUES (1, 'heads', 5.0)")
        db.execute("INSERT INTO games VALUES (2, 'heads', 7.5)")
        db.execute("INSERT INTO games VALUES (3, 'tails', 2.5)")

        rows = db.execute("SELECT coin_side, COUNT(*), SUM(amount) FROM games GROUP BY coin_side ORDER BY coin_side ASC")
        assert rows == [
            {"coin_side": "heads", "COUNT(*)": 2, "SUM(amount)": 12.5},
            {"coin_side": "tails", "COUNT(*)": 1, "SUM(amount)": 2.5},
        ]
    finally:
        db.close()


def test_count_distinct_aggregate(tmp_path):
    db_path = tmp_path / "crud_count_distinct.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE bets (id INTEGER PRIMARY KEY, username TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (1, 'alice')") == "OK"
        assert db.execute("INSERT INTO bets VALUES (2, 'alice')") == "OK"
        assert db.execute("INSERT INTO bets VALUES (3, 'bob')") == "OK"

        rows = db.execute("SELECT COUNT(DISTINCT username) AS unique_users FROM bets")
        assert rows == [{"unique_users": 2}]
    finally:
        db.close()


def test_count_case_when_expression(tmp_path):
    db_path = tmp_path / "crud_count_case_when.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE bets (id INTEGER PRIMARY KEY, win INTEGER NOT NULL)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (1, 1)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (2, 0)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (3, 1)") == "OK"

        rows = db.execute(
            "SELECT COUNT(*) AS total, "
            "COUNT(CASE WHEN win = 1 THEN 1 END) AS wins, "
            "COUNT(CASE WHEN win = 0 THEN 1 END) AS losses "
            "FROM bets"
        )
        assert rows == [{"total": 3, "wins": 2, "losses": 1}]
    finally:
        db.close()


def test_round_aggregate_expression(tmp_path):
    db_path = tmp_path / "crud_round_aggregate.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE bets (id INTEGER PRIMARY KEY, payout REAL, roll REAL)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (1, 1.111, 0.123456)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (2, 2.222, 0.654321)") == "OK"

        rows = db.execute("SELECT ROUND(AVG(payout), 2) AS avg_payout, ROUND(AVG(roll), 4) AS avg_roll FROM bets")
        assert rows == [{"avg_payout": 1.67, "avg_roll": 0.3889}]
    finally:
        db.close()


def test_having_scalar_subquery_comparison(tmp_path):
    db_path = tmp_path / "crud_having_scalar_subquery.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE bets (id INTEGER PRIMARY KEY, round_id INTEGER)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (1, 100)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (2, 100)") == "OK"
        assert db.execute("INSERT INTO bets VALUES (3, 200)") == "OK"

        rows = db.execute(
            "SELECT round_id, COUNT(*) AS c "
            "FROM bets "
            "GROUP BY round_id "
            "HAVING c = (SELECT COUNT(*) FROM bets)"
        )
        assert rows == []
    finally:
        db.close()


def test_having_correlated_scalar_subquery_with_outer_reference(tmp_path):
    db_path = tmp_path / "crud_having_correlated_scalar.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE coinflip_bets ("
            "id INTEGER PRIMARY KEY, "
            "round_id INTEGER, "
            "win INTEGER, "
            "payout REAL, "
            "roll REAL, "
            "placed_at TIMESTAMP"
            ")"
        ) == "OK"

        assert db.execute("INSERT INTO coinflip_bets VALUES (1, 10, 1, 2.0, 50.0, '2026-02-25 18:51:10')") == "OK"
        assert db.execute("INSERT INTO coinflip_bets VALUES (2, 10, 0, 0.0, 40.0, '2026-02-25 18:51:20')") == "OK"
        assert db.execute("INSERT INTO coinflip_bets VALUES (3, 11, 1, 1.0, 60.0, '2026-02-25 18:51:30')") == "OK"

        rows = db.execute(
            "SELECT COUNT(*) AS num_bets, "
            "COUNT(CASE WHEN win = 1 THEN 1 END) AS num_wins, "
            "COUNT(CASE WHEN win = 0 THEN 1 END) AS num_losses, "
            "ROUND(AVG(payout), 2) AS avg_payout, "
            "ROUND(AVG(roll), 4) AS avg_roll "
            "FROM coinflip_bets "
            "WHERE placed_at >= '2026-02-25 18:51:00' "
            "AND placed_at < '2026-02-25 18:52:00' "
            "GROUP BY round_id "
            "HAVING COUNT(*) = (SELECT COUNT(*) "
            "FROM coinflip_bets "
            "WHERE round_id = coinflip_bets.round_id "
            "AND placed_at >= '2026-02-25 18:51:00' "
            "AND placed_at < '2026-02-25 18:52:00') "
            "ORDER BY num_bets DESC "
            "LIMIT 1"
        )

        assert rows == [{"num_bets": 2, "num_wins": 1, "num_losses": 1, "avg_payout": 1.0, "avg_roll": 45.0}]
    finally:
        db.close()


def test_select_inner_join_basic(tmp_path):
    db_path = tmp_path / "crud_join_basic.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, coin_side TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO games VALUES (10, 1, 'heads')") == "OK"
        assert db.execute("INSERT INTO games VALUES (11, 2, 'tails')") == "OK"

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users JOIN games ON users.id = games.user_id "
            "ORDER BY users.name ASC"
        )
        assert rows == [
            {"users.name": "Alice", "games.coin_side": "heads"},
            {"users.name": "Bob", "games.coin_side": "tails"},
        ]
    finally:
        db.close()


def test_select_inner_join_with_where_and_limit(tmp_path):
    db_path = tmp_path / "crud_join_where_limit.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("CREATE TABLE games (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, coin_side TEXT NOT NULL)") == "OK"

        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO games VALUES (10, 1, 'heads')") == "OK"
        assert db.execute("INSERT INTO games VALUES (11, 1, 'tails')") == "OK"
        assert db.execute("INSERT INTO games VALUES (12, 2, 'heads')") == "OK"

        rows = db.execute(
            "SELECT users.name, games.coin_side "
            "FROM users JOIN games ON users.id = games.user_id "
            "WHERE users.name = 'Alice' "
            "ORDER BY games.id DESC LIMIT 1"
        )
        assert rows == [{"users.name": "Alice", "games.coin_side": "tails"}]
    finally:
        db.close()


def test_where_like_support(tmp_path):
    db_path = tmp_path / "crud_where_like.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Alicia')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Bob')") == "OK"

        rows = db.execute("SELECT id FROM users WHERE name LIKE 'Ali%' ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 2}]

        rows = db.execute("SELECT id FROM users WHERE name LIKE 'A_i_e' ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_where_not_in_support(tmp_path):
    db_path = tmp_path / "crud_where_not_in.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice')") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob')") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Cara')") == "OK"

        rows = db.execute("SELECT id FROM users WHERE id NOT IN (2, 3) ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_where_is_null_and_is_not_null(tmp_path):
    db_path = tmp_path / "crud_where_is_null.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, NULL, NULL)") == "OK"

        rows = db.execute("SELECT id FROM users WHERE name IS NULL ORDER BY id ASC")
        assert rows == [{"id": 2}]

        rows = db.execute("SELECT id FROM users WHERE score IS NOT NULL ORDER BY id ASC")
        assert rows == [{"id": 1}]
    finally:
        db.close()


def test_string_literal_escaped_single_quote(tmp_path):
    db_path = tmp_path / "crud_escaped_quote.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)") == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'O''Brien')") == "OK"

        rows = db.execute("SELECT name FROM users WHERE id = 1")
        assert rows == [{"name": "O'Brien"}]
    finally:
        db.close()


def test_where_or_and_in_support(tmp_path):
    db_path = tmp_path / "crud_where_or_in.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)"
        ) == "OK"
        assert db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)") == "OK"
        assert db.execute("INSERT INTO users VALUES (3, 'Cara', 8.1, TRUE)") == "OK"

        rows = db.execute("SELECT id FROM users WHERE id = 1 OR id = 3 ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 3}]

        rows = db.execute("SELECT id FROM users WHERE id IN (2, 3) ORDER BY id ASC")
        assert rows == [{"id": 2}, {"id": 3}]

        rows = db.execute("SELECT id FROM users WHERE active = TRUE AND id IN (1, 3) ORDER BY id ASC")
        assert rows == [{"id": 1}, {"id": 3}]
    finally:
        db.close()


def test_timestamp_type_round_trip(tmp_path):
    db_path = tmp_path / "crud_timestamp.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL, note TEXT)"
        ) == "OK"

        assert db.execute(
            "INSERT INTO events VALUES (1, '2023-04-01 12:34:56', 'boot')"
        ) == "OK"

        rows = db.execute("SELECT created_at, note FROM events WHERE id = 1")
        assert rows == [{"created_at": "2023-04-01 12:34:56", "note": "boot"}]
    finally:
        db.close()


def test_multi_row_insert(tmp_path):
    db_path = tmp_path / "crud_multi_insert.db"
    db = TinyDB(str(db_path))
    try:
        assert db.execute(
            "CREATE TABLE games (id INTEGER PRIMARY KEY, coin_side TEXT NOT NULL, bet_amount REAL, outcome BOOLEAN)"
        ) == "OK"

        assert db.execute(
            "INSERT INTO games (id, coin_side, bet_amount, outcome) VALUES "
            "(1, 'heads', 5.0, TRUE), "
            "(2, 'tails', 3.5, FALSE), "
            "(3, 'heads', 7.2, TRUE)"
        ) == "OK"

        rows = db.execute("SELECT * FROM games ORDER BY id ASC")
        assert rows == [
            {"id": 1, "coin_side": "heads", "bet_amount": 5.0, "outcome": True},
            {"id": 2, "coin_side": "tails", "bet_amount": 3.5, "outcome": False},
            {"id": 3, "coin_side": "heads", "bet_amount": 7.2, "outcome": True},
        ]
    finally:
        db.close()

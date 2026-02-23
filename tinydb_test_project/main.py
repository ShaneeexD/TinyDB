from tinydb_engine import TinyDB


def run_demo() -> None:
    db = TinyDB("demo_app.db")
    try:
        try:
            db.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)"
            )
        except ValueError as exc:
            if "Table already exists" not in str(exc):
                raise

        existing = db.execute("SELECT id FROM users LIMIT 1")
        if not existing:
            db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)")
            db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)")
            db.execute("INSERT INTO users VALUES (3, 'Cara', 8.8, TRUE)")

        print("Top active users:")
        rows = db.execute(
            "SELECT id, name, score FROM users WHERE active = TRUE ORDER BY score DESC LIMIT 10"
        )
        for row in rows:
            print(row)

        db.execute("UPDATE users SET score = 7.8 WHERE id = 2")
        db.execute("DELETE FROM users WHERE id = 1")

        print("Remaining rows:")
        for row in db.execute("SELECT * FROM users ORDER BY id ASC"):
            print(row)
    finally:
        db.close()


if __name__ == "__main__":
    run_demo()

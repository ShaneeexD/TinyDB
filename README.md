# tinydb_engine

`tinydb_engine` is a simple embedded database engine in Python, inspired by SQLite concepts.

It stores data in a single file, uses fixed-size pages, maintains a primary-key B-tree index, and includes a write-ahead log (WAL) for crash recovery.

## Features

- Single-file on-disk database
- Fixed-size page storage (4096 bytes)
- SQL-like commands:
  - `CREATE TABLE`
  - `INSERT INTO ... VALUES`
  - `SELECT ... [WHERE] [ORDER BY] [LIMIT]`
  - `UPDATE ... SET ... [WHERE]`
  - `DELETE FROM ... [WHERE]`
  - `DROP TABLE ...`
  - `ALTER TABLE ... RENAME TO ...`
  - `ALTER TABLE ... RENAME COLUMN ... TO ...`
  - `ALTER TABLE ... ADD COLUMN ...`
  - `ALTER TABLE ... REMOVE COLUMN ...`
  - `SHOW TABLES`
  - `DESCRIBE table_name`
  - `BEGIN`, `COMMIT`, `ROLLBACK`
- Data types:
  - `INTEGER`, `TEXT`, `REAL`, `BOOLEAN`, `TIMESTAMP`
- Constraints:
  - single-column `PRIMARY KEY`
  - `NOT NULL`
  - `FOREIGN KEY (col) REFERENCES other_table(other_col)`
  - Note: `ALTER TABLE ... ADD COLUMN` currently allows nullable, non-PK columns only.
  - Note: `ALTER TABLE ... REMOVE COLUMN` currently supports removing only the last non-PK column.
- Primary key B-tree index (with PK equality lookup fast path)
- WAL-based crash recovery
- Python API + interactive REPL CLI

## Install

### Quick install (Windows, easiest)

From this repo root, double-click:

- `install.bat`

This creates a local `.venv` and installs `tinydb_engine` in editable mode.

### Manual install (PowerShell)

From this repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\python -m pip install -e .
```

This installs:
- the importable package (`tinydb_engine`)
- the console command (`tinydb`)
- the GUI command (`tinydb-gui`)

## Quick Start (Python API)

```python
from tinydb_engine import TinyDB, hash_password, verify_password

db = TinyDB("myfile.db")

db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, active BOOLEAN)")
db.execute("INSERT INTO users VALUES (1, 'Alice', 9.5, TRUE)")
db.execute("INSERT INTO users VALUES (2, 'Bob', 7.0, FALSE)")

rows = db.execute("SELECT * FROM users WHERE score >= 7.5 ORDER BY score DESC LIMIT 10")
print(rows)

updated = db.execute("UPDATE users SET score = 8.1 WHERE id = 2")
print(updated)

deleted = db.execute("DELETE FROM users WHERE id = 1")
print(deleted)

db.close()
```

## Password Hashing (for user auth)

TinyDB now includes salted PBKDF2 hashing helpers so you can store password hashes instead of raw passwords:

```python
from tinydb_engine import TinyDB, hash_password, verify_password

db = TinyDB("users.db")
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, password_hash TEXT NOT NULL)")

password_hash = hash_password("my-plain-password")
db.execute(f"INSERT INTO users VALUES (1, 'alice', '{password_hash}')")

stored = db.execute("SELECT password_hash FROM users WHERE id = 1")[0]["password_hash"]
is_valid = verify_password("my-plain-password", stored)
print(is_valid)  # True

db.close()
```

Notes:
- Never store raw passwords.
- Keep application-level auth controls (sessions, lockouts, resets) outside the DB engine.

Convenience helpers on `TinyDB`:

```python
db.create_user("alice", "my-plain-password")
ok = db.authenticate_user("alice", "my-plain-password")
print(ok)  # True
```

## REPL / CLI

Run REPL via module:

```powershell
python -m tinydb_engine.repl myfile.db
```

Or via installed console command:

```powershell
tinydb myfile.db
```

REPL helpers:
- `.tables`
- `.schema`
- `.help`
- `.exit`

`SELECT` results are rendered as an ASCII table for easier visual inspection.

Example:

```text
+----+-------+-------+--------+
| id | name  | score | active |
+----+-------+-------+--------+
| 1  | Alice | 9.5   | TRUE   |
| 2  | Bob   | 7.0   | FALSE  |
+----+-------+-------+--------+
(2 row(s))
```

## GUI Viewer

Launch the visual GUI (file picker + tables list + schema pane + SQL console):

```powershell
tinydb-gui
```

Or open a DB directly:

```powershell
tinydb-gui myfile.db
```

Without script installation:

```powershell
python -m tinydb_engine.gui myfile.db
```

In the GUI:
- click a table once to view its schema
- double-click a table to open a full row browser window (no `SELECT *` needed)
- in the row browser, select a row and click `Edit Selected Row` to update values

### Windows GUI helper scripts

- `install_gui_deps.bat`  
  Creates `.venv` if needed, upgrades pip, installs package for GUI use.
- `run_gui.bat`  
  Runs `python -m tinydb_engine.gui` via `.venv`, forwarding optional DB path args.

Example:

```powershell
run_gui.bat myfile.db
```

## Running Tests

From repo root:

```powershell
python -m pytest -q
```

Run one test file:

```powershell
python -m pytest tests/test_crud.py -q
```

## Build a Distribution

```powershell
python -m pip install build
python -m build
```

Artifacts will be generated in `dist/` (wheel + sdist).

Install wheel example:

```powershell
python -m pip install .\dist\tinydb_engine-0.1.0-py3-none-any.whl
```

## Current MVP Notes / Limitations

- `WHERE` supports `AND`, `OR`, and `IN (...)` predicates.
- `PRIMARY KEY` support is single-column.
- `ORDER BY` is in-memory sort.
- Transactions are implicit per statement unless explicit `BEGIN ... COMMIT/ROLLBACK` is used.
- SQL parser is intentionally small and supports a practical subset.

## Project Layout

```text
tinydb_engine/
  api.py
  parser.py
  ast_nodes.py
  executor.py
  schema.py
  repl.py
  storage/
    pager.py
    record.py
    catalog.py
  index/
    btree.py
  wal/
    wal.py
tests/
  test_crud.py
  test_constraints.py
  test_select_features.py
  test_recovery.py
```

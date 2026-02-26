# tinydb_engine

`tinydb_engine` is a simple embedded database engine in Python, inspired by SQLite concepts.

It stores data in a single file, uses fixed-size pages, maintains a primary-key B-tree index, and includes a write-ahead log (WAL) for crash recovery.

## Features

- Single-file on-disk database
- Fixed-size page storage (4096 bytes)
- SQL-like commands:
  - `CREATE TABLE` (supports `IF NOT EXISTS`)
  - `INSERT INTO ... VALUES`
  - `SELECT [DISTINCT] ... [WHERE] [GROUP BY ...] [HAVING ...] [ORDER BY] [LIMIT]`
  - `UPDATE ... SET ... [WHERE]`
  - `DELETE FROM ... [WHERE]`
  - `DROP TABLE ...`
  - `ALTER TABLE ... RENAME TO ...`
  - `ALTER TABLE ... RENAME COLUMN ... TO ...`
  - `ALTER TABLE ... ADD COLUMN ...`
  - `ALTER TABLE ... REMOVE COLUMN ...`
  - `CREATE INDEX ... ON table_name(column_name[, column_name ...])`
  - `DROP INDEX index_name`
  - `SHOW TABLES`
  - `SHOW INDEXES [table_name]`
  - `DESCRIBE table_name`
  - `EXPLAIN SELECT ...`
  - `BEGIN`, `COMMIT`, `ROLLBACK`
- Data types:
  - `INTEGER`, `TEXT`, `REAL`, `BOOLEAN`, `TIMESTAMP`, `BLOB`, `DECIMAL` (`NUMERIC` alias)
- Constraints:
  - `PRIMARY KEY` (single-column and table-level composite, e.g. `PRIMARY KEY (user_id, org_id)`)
  - `INTEGER PRIMARY KEY AUTOINCREMENT`
  - `NOT NULL`
  - `FOREIGN KEY (col) REFERENCES other_table(other_col)`
  - Note: `ALTER TABLE ... ADD COLUMN` currently allows nullable, non-PK columns only.
  - Note: `ALTER TABLE ... REMOVE COLUMN` currently supports removing only the last non-PK column.
- Primary key B-tree index (with PK equality lookup fast path)
- Header metadata overflow handling (large schema/index metadata spills into overflow pages)
- WAL-based crash recovery
- Python API + interactive REPL CLI

## Good fit for these app types

`tinydb_engine` is best for lightweight embedded storage where simple setup matters more than enterprise-scale operations.

Great fits:
- Indie games (save data, player inventories, match history, local leaderboards)
- Desktop tools and utilities (notes, configs, local catalogs, offline-first apps)
- Small web apps and dashboards with modest traffic
- Internal tools and prototypes that need SQL support without running a separate DB server
- Education and experimentation (learning database internals and SQL execution flow)

Less ideal:
- High-concurrency, high-throughput multi-tenant backend systems
- Large distributed systems that need deep operational tooling and horizontal scaling

## Install

### Windows quick install

From repo root, run:

```powershell
install.bat
```

### Manual install

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\python -m pip install -e .
```

This gives you:
- Python package: `tinydb_engine`
- CLI command: `tinydb`
- GUI command: `tinydb-gui`

## Use from Python

```python
from tinydb_engine import TinyDB

db = TinyDB("app.db")

db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE, name TEXT)")
db.execute("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice')")
db.execute("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob')")

rows = db.execute("SELECT id, name FROM users WHERE email LIKE '%@example.com' ORDER BY id ASC")
print(rows)

db.execute("CREATE INDEX idx_users_email ON users(email)")
print(db.execute("SHOW INDEXES users"))
print(db.execute("EXPLAIN SELECT id FROM users WHERE email = 'alice@example.com'"))

db.close()
```

## SQL examples you can run

```sql
CREATE TABLE IF NOT EXISTS games (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, result TEXT);
INSERT INTO games VALUES (10, 1, 'W'), (11, 2, 'L');

SELECT users.name, games.result
FROM users LEFT JOIN games ON users.id = games.user_id
ORDER BY users.id ASC;

SELECT result, COUNT(*)
FROM games
GROUP BY result;

SHOW TABLES;
SHOW INDEXES users;
DESCRIBE users;
```

## Custom keywords (TinyDB-specific)

These commands are useful for inspecting and profiling your database quickly:

- `SHOW TABLES`  
  Lists tables in the current DB.
- `SHOW INDEXES [table_name]`  
  Lists secondary indexes (optionally for one table).
- `SHOW STATS`  
  Returns a quick DB summary (`table_count`, `index_count`, `row_count`, `page_count`, `file_size_bytes`).
- `DESCRIBE table_name`  
  Shows column metadata (type, PK, NULL, default, FK, indexes).
- `EXPLAIN SELECT ...`  
  Shows the chosen plan label plus basic estimates (`estimated_rows`, `estimated_cost`).
- `PROFILE SELECT ...`  
  Runs the query and returns timing + row count + plan.

Example:

```sql
SHOW STATS;
SHOW INDEXES users;
EXPLAIN SELECT id FROM users WHERE id = 1;
PROFILE SELECT id, name FROM users ORDER BY id ASC LIMIT 50;
```

## CLI / REPL

```powershell
tinydb app.db
```

Or:

```powershell
python -m tinydb_engine.repl app.db
```

## GUI

```powershell
tinydb-gui
```

Or with a specific DB:

```powershell
tinydb-gui app.db
```

Windows helpers:

```powershell
install_gui_deps.bat
run_gui.bat app.db
```

## Notes

- Transactions: implicit per statement, or explicit `BEGIN` / `COMMIT` / `ROLLBACK`.
- Supported predicates: `AND`, `OR`, `IN`, `NOT IN`, `IN (SELECT ...)`, `NOT IN (SELECT ...)`, `LIKE`, `IS NULL`, `IS NOT NULL`.
- Joins: chained `JOIN`, `INNER JOIN`, and `LEFT JOIN` (equality `ON`) with optional table aliases.
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (with `GROUP BY`, `HAVING`).
- Types: `INTEGER`, `TEXT`, `REAL`, `BOOLEAN`, `TIMESTAMP`, `BLOB`, `DECIMAL` (`NUMERIC` alias).
- Binary-safe BLOB inserts are best done with parameter binding (`?`) so arbitrary bytes are preserved.

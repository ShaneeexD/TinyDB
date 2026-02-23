# tinydb_test_project

Minimal consumer project used to validate that `tinydb_engine` can be installed and used from another project.

## Setup

From this folder (`d:\Projects\TinyDB\tinydb_test_project`):

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\python -m pip install -e ..
```

## Run

```powershell
.\.venv\Scripts\python main.py
```

This creates `demo_app.db` and performs create/insert/select/update/delete operations.

## View the existing database (REPL)

Use the same venv interpreter that has `tinydb_engine` installed:

```powershell
.\.venv\Scripts\python -m tinydb_engine.repl demo_app.db
```

Or call the console script directly from the venv:

```powershell
.\.venv\Scripts\tinydb.exe demo_app.db
```

Inside REPL, try:

```text
.tables
.schema
SELECT * FROM users ORDER BY id;
```

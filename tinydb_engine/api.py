from __future__ import annotations

from typing import Any

from tinydb_engine.executor import Executor
from tinydb_engine.parser import parse
from tinydb_engine.security import hash_password, verify_password
from tinydb_engine.storage.pager import Pager
from tinydb_engine.wal.wal import WAL


class TinyDB:
    def __init__(self, path: str):
        self.path = path
        self.wal = WAL(path)
        self.pager = Pager(path, wal=self.wal)
        self.executor = Executor(self.pager)

    def execute(self, sql: str) -> Any:
        stmt = parse(sql)
        self.pager.begin()
        try:
            result = self.executor.execute(stmt)
            self.pager.commit()
            return result
        except Exception:
            self.pager.rollback()
            raise

    def create_user(self, username: str, password: str, table_name: str = "users") -> str:
        clean_username = username.strip()
        if not clean_username:
            raise ValueError("Username cannot be empty")
        if "'" in clean_username:
            raise ValueError("Username cannot contain single quotes")

        password_hash = hash_password(password)
        schema = self.executor.schemas.get(table_name.lower())
        if schema is None:
            raise ValueError(f"Unknown table: {table_name}")

        pk_col = schema.pk_column
        if pk_col is None:
            sql = (
                f"INSERT INTO {table_name} (username, password_hash) "
                f"VALUES ('{clean_username}', '{password_hash}')"
            )
        elif pk_col.name.lower() == "id" and pk_col.data_type == "INTEGER":
            rows = self.execute(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1")
            next_id = 1 if not rows else int(rows[0]["id"]) + 1
            sql = (
                f"INSERT INTO {table_name} (id, username, password_hash) "
                f"VALUES ({next_id}, '{clean_username}', '{password_hash}')"
            )
        else:
            raise ValueError("create_user supports tables without PK or with INTEGER PRIMARY KEY id")
        result = self.execute(sql)
        return str(result)

    def authenticate_user(self, username: str, password: str, table_name: str = "users") -> bool:
        clean_username = username.strip()
        if not clean_username or "'" in clean_username:
            return False

        rows = self.execute(
            f"SELECT password_hash FROM {table_name} "
            f"WHERE username = '{clean_username}' LIMIT 1"
        )
        if not rows:
            return False

        stored_hash = rows[0].get("password_hash")
        if not isinstance(stored_hash, str):
            return False
        return verify_password(password, stored_hash)

    def close(self) -> None:
        self.pager.close()

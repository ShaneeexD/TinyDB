from __future__ import annotations

from decimal import Decimal
from typing import Any, Sequence

from tinydb_engine.ast_nodes import BeginStmt, CommitStmt, RollbackStmt
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
        self._explicit_tx_active = False

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        if params is not None:
            sql = self._bind_params(sql, params)
        stmt = parse(sql)

        if isinstance(stmt, BeginStmt):
            if self._explicit_tx_active:
                raise ValueError("Transaction already active")
            self.pager.begin()
            self._explicit_tx_active = True
            return "OK"
        if isinstance(stmt, CommitStmt):
            if not self._explicit_tx_active:
                raise ValueError("No active transaction to COMMIT")
            self.pager.commit()
            self._explicit_tx_active = False
            return "OK"
        if isinstance(stmt, RollbackStmt):
            if not self._explicit_tx_active:
                raise ValueError("No active transaction to ROLLBACK")
            self.pager.rollback()
            self._explicit_tx_active = False
            return "OK"

        if self._explicit_tx_active:
            return self.executor.execute(stmt)

        self.pager.begin()
        try:
            result = self.executor.execute(stmt)
            self.pager.commit()
            return result
        except Exception:
            self.pager.rollback()
            raise

    def _bind_params(self, sql: str, params: Sequence[Any]) -> str:
        pieces: list[str] = []
        param_idx = 0
        in_string = False
        i = 0
        while i < len(sql):
            ch = sql[i]
            if ch == "'":
                if in_string and i + 1 < len(sql) and sql[i + 1] == "'":
                    pieces.append("''")
                    i += 2
                    continue
                in_string = not in_string
                pieces.append(ch)
                i += 1
                continue
            if ch == "?" and not in_string:
                if param_idx >= len(params):
                    raise ValueError("Not enough parameters for SQL placeholders")
                pieces.append(self._to_sql_literal(params[param_idx]))
                param_idx += 1
                i += 1
                continue
            pieces.append(ch)
            i += 1

        if param_idx != len(params):
            raise ValueError("Too many parameters for SQL placeholders")
        return "".join(pieces)

    def _to_sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, Decimal):
            return f"'{str(value)}'"
        if isinstance(value, (bytes, bytearray)):
            text = bytes(value).decode("utf-8")
            text = text.replace("'", "''")
            return f"'{text}'"
        if isinstance(value, (int, float)):
            return str(value)

        text = str(value)
        text = text.replace("'", "''")
        return f"'{text}'"

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

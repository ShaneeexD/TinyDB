from __future__ import annotations

import argparse
from typing import Any

from tinydb_engine.api import TinyDB


def _format_scalar(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _format_rows_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "(0 rows)"

    columns = list(rows[0].keys())
    rendered_rows = [[_format_scalar(row.get(col)) for col in columns] for row in rows]

    widths = []
    for idx, col in enumerate(columns):
        cell_width = max(len(r[idx]) for r in rendered_rows) if rendered_rows else 0
        widths.append(max(len(col), cell_width))

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "| " + " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns)) + " |"
    body = [
        "| " + " | ".join(values[i].ljust(widths[i]) for i in range(len(columns))) + " |"
        for values in rendered_rows
    ]

    return "\n".join([border, header, border, *body, border, f"({len(rows)} row(s))"])


def main() -> None:
    parser = argparse.ArgumentParser(description="tinydb_engine REPL")
    parser.add_argument("db_path", help="Database file path")
    args = parser.parse_args()

    db = TinyDB(args.db_path)
    print("tinydb_engine REPL. Commands: .tables, .schema, .help, .exit")
    try:
        while True:
            line = input("tinydb> ").strip()
            if not line:
                continue
            if line in {".exit", ".quit"}:
                break
            if line == ".tables":
                names = sorted(table.name for table in db.executor.schemas.values())
                if not names:
                    print("(no tables)")
                else:
                    for name in names:
                        print(name)
                continue
            if line == ".schema":
                schemas = db.executor.schemas
                if not schemas:
                    print("(no schema)")
                    continue
                for table in schemas.values():
                    cols = ", ".join(
                        f"{c.name} {c.data_type}{' PRIMARY KEY' if c.primary_key else ''}{' NOT NULL' if c.not_null and not c.primary_key else ''}"
                        for c in table.columns
                    )
                    print(f"{table.name}: {cols}")
                continue
            if line == ".help":
                print("Commands: .tables, .schema, .help, .exit")
                print("Tip: SELECT results are shown in a table.")
                continue
            try:
                result = db.execute(line)
                if isinstance(result, list) and (not result or isinstance(result[0], dict)):
                    print(_format_rows_table(result))
                else:
                    print(result)
            except Exception as exc:
                print(f"error: {exc}")
    finally:
        db.close()


if __name__ == "__main__":
    main()

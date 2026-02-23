from __future__ import annotations

import argparse
import json
import logging
import os
import re
import threading
import tkinter as tk
import traceback
import urllib.error
import urllib.request
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any

from tinydb_engine import TinyDB
from tinydb_engine.parser import ParseError


SQL_KEYWORDS = {
    "ADD",
    "ALTER",
    "AND",
    "ASC",
    "BY",
    "COLUMN",
    "CREATE",
    "DELETE",
    "DROP",
    "DESC",
    "FALSE",
    "FROM",
    "INSERT",
    "INTO",
    "KEY",
    "LIMIT",
    "NOT",
    "NULL",
    "ORDER",
    "PRIMARY",
    "REMOVE",
    "RENAME",
    "SELECT",
    "SET",
    "TABLE",
    "TO",
    "TRUE",
    "UPDATE",
    "VALUES",
    "WHERE",
}

SQL_TYPES = {"INTEGER", "TEXT", "REAL", "BOOLEAN", "TIMESTAMP"}

CLAUDE_MODEL = "claude-3-haiku-20240307"
AI_SAMPLE_ROW_LIMIT = 3
QUERY_HISTORY_LIMIT = 100
GUI_LOG_FILE = "tinydb_gui.log"
GUI_CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".tinydb_gui_config.json")

CLAUDE_SYSTEM_PROMPT = """You are an assistant that writes SQL for tinydb_engine only.

Return exactly one SQL statement and nothing else (no markdown, no prose, no code fences).
Use only syntax supported by tinydb_engine:
- CREATE TABLE
- FOREIGN KEY (col) REFERENCES other_table(other_col) inside CREATE TABLE
- INSERT INTO ... VALUES (...) or INSERT INTO ... VALUES (...), (...)
- SELECT ... [WHERE] [ORDER BY] [LIMIT]
- UPDATE ... SET ... [WHERE]
- DELETE FROM ... [WHERE]
- DROP TABLE ...
- ALTER TABLE ... RENAME TO ...
- ALTER TABLE ... RENAME COLUMN ... TO ...
- ALTER TABLE ... ADD COLUMN ...
- ALTER TABLE ... REMOVE COLUMN ...

Important limitations:
- Available SQL column types: INTEGER, TEXT, REAL, BOOLEAN, TIMESTAMP.
- TIMESTAMP values should be string literals (example: '2023-04-01 12:34:56').
- WHERE supports AND-combined predicates only.
- ALTER TABLE ADD COLUMN supports nullable non-PK columns only.
- ALTER TABLE REMOVE COLUMN supports only removing the last non-PK column.
- Identifiers must match schema names exactly (e.g. player_id, not "player id").

Generate practical SQL using available schema context.
"""


def _scalar(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _format_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "(0 rows)"

    columns = list(rows[0].keys())
    rendered_rows = [[_scalar(row.get(col)) for col in columns] for row in rows]
    widths = [max(len(columns[i]), max(len(r[i]) for r in rendered_rows)) for i in range(len(columns))]

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "| " + " | ".join(columns[i].ljust(widths[i]) for i in range(len(columns))) + " |"
    body = [
        "| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(columns))) + " |"
        for row in rendered_rows
    ]
    return "\n".join([border, header, border, *body, border, f"({len(rows)} row(s))"])


def _parse_editor_value(data_type: str, text: str) -> Any:
    raw = text.strip()
    if raw.upper() == "NULL":
        return None
    if data_type == "INTEGER":
        return int(raw)
    if data_type == "REAL":
        return float(raw)
    if data_type == "BOOLEAN":
        lowered = raw.lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
        raise ValueError("BOOLEAN values must be TRUE/FALSE/1/0 or NULL")
    return text


def _to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if "'" in text:
        raise ValueError("Single quotes are not yet supported in edited TEXT values")
    return f"'{text}'"


class TinyDBGui:
    def __init__(self, root: tk.Tk, db_path: str | None):
        self.root = root
        self.root.title("TinyDB Viewer")
        self.root.geometry("1200x820")
        self.root.configure(bg="#f6f8fb")

        self._config = self._load_config()

        self.db: TinyDB | None = None
        configured_db_path = str(self._config.get("last_db_path", "") or "")
        self.db_path_var = tk.StringVar(value=db_path or configured_db_path)
        self.claude_api_key_var = tk.StringVar(value=str(self._config.get("api_key", "") or ""))
        self.claude_model_var = tk.StringVar(value=str(self._config.get("model", CLAUDE_MODEL) or CLAUDE_MODEL))
        saved_history = self._config.get("sql_history", [])
        self.query_history: list[str] = [str(item) for item in saved_history if isinstance(item, str)]
        saved_snippets = self._config.get("sql_snippets", [])
        self.query_snippets: list[str] = [str(item) for item in saved_snippets if isinstance(item, str)]
        self.history_expanded_var = tk.BooleanVar(value=False)
        self.snippets_expanded_var = tk.BooleanVar(value=False)
        self.ai_expanded_var = tk.BooleanVar(value=False)
        self.autocomplete_popup: tk.Toplevel | None = None
        self.autocomplete_list: tk.Listbox | None = None
        self.ai_request_inflight = False
        self.log_file_path = os.path.abspath(GUI_LOG_FILE)
        self.logger = self._build_logger()

        self._configure_style()
        self._build_ui()
        self._print_output(f"GUI log file: {self.log_file_path}", level="INFO")
        self.claude_api_key_var.trace_add("write", self._on_api_key_changed)
        self.claude_model_var.trace_add("write", self._on_model_changed)
        if db_path:
            self.open_db(db_path)
        elif configured_db_path and os.path.exists(configured_db_path):
            self.open_db(configured_db_path)

    def _load_config(self) -> dict[str, Any]:
        try:
            if not os.path.exists(GUI_CONFIG_FILE):
                return {}
            with open(GUI_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as exc:
            self._log_exception("Load GUI config failed", exc) if hasattr(self, "logger") else None
        return {}

    def _save_config(self) -> None:
        data = {
            "api_key": self.claude_api_key_var.get().strip(),
            "model": self.claude_model_var.get().strip() or CLAUDE_MODEL,
            "last_db_path": self.db_path_var.get().strip(),
            "sql_history": self.query_history[:QUERY_HISTORY_LIMIT],
            "sql_snippets": self.query_snippets[:QUERY_HISTORY_LIMIT],
        }
        try:
            with open(GUI_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as exc:
            self._log_exception("Save GUI config failed", exc)

    def _on_api_key_changed(self, *_args: Any) -> None:
        self._save_config()

    def _on_model_changed(self, *_args: Any) -> None:
        self._save_config()

    def _refresh_history_list(self) -> None:
        self.history_list.delete(0, tk.END)
        for sql in self.query_history[:QUERY_HISTORY_LIMIT]:
            self.history_list.insert(tk.END, sql)

    def _toggle_history_visibility(self) -> None:
        if self.history_expanded_var.get():
            self.history_body.pack(fill=tk.X, padx=8, pady=(4, 6))
        else:
            self.history_body.pack_forget()

    def _record_query_history(self, sql: str) -> None:
        cleaned = sql.strip()
        if not cleaned:
            return
        self.query_history = [item for item in self.query_history if item != cleaned]
        self.query_history.insert(0, cleaned)
        self.query_history = self.query_history[:QUERY_HISTORY_LIMIT]
        self._refresh_history_list()
        self._save_config()

    def _selected_history_sql(self) -> str | None:
        selection = self.history_list.curselection()
        if not selection:
            return None
        return str(self.history_list.get(selection[0]))

    def _use_selected_history(self) -> None:
        sql = self._selected_history_sql()
        if sql is None:
            messagebox.showinfo("SQL History", "Select a history entry first.")
            return
        self._set_query_text(sql)

    def _run_selected_history(self) -> None:
        sql = self._selected_history_sql()
        if sql is None:
            messagebox.showinfo("SQL History", "Select a history entry first.")
            return
        self._set_query_text(sql)
        self._execute_sql_text(sql)

    def _on_history_double_click(self, _event: Any) -> None:
        self._use_selected_history()

    def _refresh_snippet_list(self) -> None:
        self.snippet_list.delete(0, tk.END)
        for sql in self.query_snippets[:QUERY_HISTORY_LIMIT]:
            self.snippet_list.insert(tk.END, sql)

    def _toggle_snippets_visibility(self) -> None:
        if self.snippets_expanded_var.get():
            self.snippet_body.pack(fill=tk.X, padx=8, pady=(4, 6))
        else:
            self.snippet_body.pack_forget()

    def _toggle_ai_visibility(self) -> None:
        if self.ai_expanded_var.get():
            self.ai_body.pack(fill=tk.X, padx=8, pady=(4, 6))
        else:
            self.ai_body.pack_forget()

    def _selected_snippet_sql(self) -> str | None:
        selection = self.snippet_list.curselection()
        if not selection:
            return None
        return str(self.snippet_list.get(selection[0]))

    def _save_current_snippet(self) -> None:
        sql = self.query_entry.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showinfo("Saved SQL Snippets", "Enter SQL in the editor first.")
            return
        self.query_snippets = [item for item in self.query_snippets if item != sql]
        self.query_snippets.insert(0, sql)
        self.query_snippets = self.query_snippets[:QUERY_HISTORY_LIMIT]
        self._refresh_snippet_list()
        self._save_config()
        self._print_output("Saved SQL snippet.", level="INFO")

    def _use_selected_snippet(self) -> None:
        sql = self._selected_snippet_sql()
        if sql is None:
            messagebox.showinfo("Saved SQL Snippets", "Select a snippet first.")
            return
        self._set_query_text(sql)

    def _run_selected_snippet(self) -> None:
        sql = self._selected_snippet_sql()
        if sql is None:
            messagebox.showinfo("Saved SQL Snippets", "Select a snippet first.")
            return
        self._set_query_text(sql)
        self._execute_sql_text(sql)

    def _delete_selected_snippet(self) -> None:
        sql = self._selected_snippet_sql()
        if sql is None:
            messagebox.showinfo("Saved SQL Snippets", "Select a snippet first.")
            return
        self.query_snippets = [item for item in self.query_snippets if item != sql]
        self._refresh_snippet_list()
        self._save_config()

    def _on_snippet_double_click(self, _event: Any) -> None:
        self._use_selected_snippet()

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger("tinydb_engine.gui")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        has_file_handler = False
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None) == self.log_file_path:
                has_file_handler = True
                break

        if not has_file_handler:
            handler = logging.FileHandler(self.log_file_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(handler)
        return logger

    def _configure_style(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        self.root.option_add("*Font", "{Segoe UI} 10")
        style.configure("TFrame", background="#f6f8fb")
        style.configure("TLabelframe", background="#f6f8fb")
        style.configure("TLabelframe.Label", background="#f6f8fb", font=("Segoe UI", 10, "bold"))
        style.configure("Header.TLabel", font=("Segoe UI", 11, "bold"), background="#f6f8fb")
        style.configure("TButton", padding=(10, 5))

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill=tk.X)

        ttk.Label(top, text="DB File:").pack(side=tk.LEFT)
        ttk.Entry(top, textvariable=self.db_path_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(top, text="New DB", command=self._create_new_db).pack(side=tk.LEFT, padx=4)
        ttk.Button(top, text="Browse", command=self._browse).pack(side=tk.LEFT, padx=4)
        ttk.Button(top, text="Open", command=lambda: self.open_db(self.db_path_var.get())).pack(side=tk.LEFT)

        split = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        split.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        left = ttk.Frame(split)
        right = ttk.Frame(split)
        split.add(left, weight=1)
        split.add(right, weight=3)

        ttk.Label(left, text="Tables", style="Header.TLabel").pack(anchor=tk.W)
        self.table_list = tk.Listbox(left, height=12)
        self.table_list.pack(fill=tk.BOTH, expand=True)
        self.table_list.configure(bg="#ffffff", relief=tk.FLAT, highlightthickness=1, highlightbackground="#d7dce3")
        self.table_list.bind("<<ListboxSelect>>", self._on_table_select)
        self.table_list.bind("<Double-Button-1>", self._on_table_double_click)
        self.table_list.bind("<Button-3>", self._on_table_right_click)

        self.table_menu = tk.Menu(self.root, tearoff=0)
        self.table_menu.add_command(label="Rename Table...", command=self._rename_selected_table)

        ttk.Label(left, text="Tip: double-click a table to view all rows").pack(anchor=tk.W, pady=(4, 0))

        ttk.Label(left, text="Schema", style="Header.TLabel").pack(anchor=tk.W, pady=(8, 0))
        self.schema_text = tk.Text(left, height=7, wrap=tk.WORD)
        self.schema_text.pack(fill=tk.X)
        self.schema_text.configure(bg="#ffffff", relief=tk.FLAT, padx=8, pady=6)
        self.schema_text.configure(state=tk.DISABLED)

        query_frame = ttk.Frame(right)
        query_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(query_frame, text="SQL Console", style="Header.TLabel").pack(anchor=tk.W)
        self.query_entry = tk.Text(query_frame, height=5, wrap=tk.WORD)
        self.query_entry.pack(fill=tk.X)
        self.query_entry.configure(bg="#ffffff", relief=tk.FLAT, padx=8, pady=6, font=("Consolas", 10))
        self.query_entry.insert("1.0", "SELECT * FROM users LIMIT 20;")
        self.query_entry.bind("<KeyRelease>", self._on_query_key_release)
        self.query_entry.bind("<Tab>", self._on_query_tab)
        self.query_entry.bind("<Down>", self._on_query_down)
        self.query_entry.bind("<Up>", self._on_query_up)
        self.query_entry.bind("<Escape>", self._on_query_escape)
        self.query_entry.bind("<Button-1>", self._on_query_click)

        self.query_entry.tag_configure("sql_keyword", foreground="#1155cc")
        self.query_entry.tag_configure("sql_type", foreground="#0b8043")
        self.query_entry.tag_configure("sql_string", foreground="#a1421d")
        self.query_entry.tag_configure("sql_number", foreground="#7b1fa2")
        self._highlight_sql()

        btns = ttk.Frame(query_frame)
        btns.pack(fill=tk.X, pady=6)
        ttk.Button(btns, text="Run SQL", command=self.run_sql).pack(side=tk.LEFT)
        ttk.Button(btns, text="Refresh Metadata", command=self.refresh_metadata).pack(side=tk.LEFT, padx=6)

        history_frame = ttk.LabelFrame(query_frame, text="SQL History")
        history_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Checkbutton(
            history_frame,
            text="Show history",
            variable=self.history_expanded_var,
            command=self._toggle_history_visibility,
        ).pack(anchor=tk.W, padx=8, pady=(6, 0))

        self.history_body = ttk.Frame(history_frame)
        self.history_body.pack(fill=tk.X, padx=8, pady=(4, 6))

        self.history_list = tk.Listbox(self.history_body, height=4)
        self.history_list.pack(fill=tk.X, pady=(0, 4))
        self.history_list.configure(bg="#ffffff", relief=tk.FLAT, highlightthickness=1, highlightbackground="#d7dce3")
        self.history_list.bind("<Double-Button-1>", self._on_history_double_click)

        history_btns = ttk.Frame(self.history_body)
        history_btns.pack(fill=tk.X)
        ttk.Button(history_btns, text="Use Selected", command=self._use_selected_history).pack(side=tk.LEFT)
        ttk.Button(history_btns, text="Run Selected", command=self._run_selected_history).pack(side=tk.LEFT, padx=6)
        self._refresh_history_list()
        self._toggle_history_visibility()

        snippet_frame = ttk.LabelFrame(query_frame, text="Saved SQL Snippets")
        snippet_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Checkbutton(
            snippet_frame,
            text="Show snippets",
            variable=self.snippets_expanded_var,
            command=self._toggle_snippets_visibility,
        ).pack(anchor=tk.W, padx=8, pady=(6, 0))

        self.snippet_body = ttk.Frame(snippet_frame)
        self.snippet_body.pack(fill=tk.X, padx=8, pady=(4, 6))

        self.snippet_list = tk.Listbox(self.snippet_body, height=4)
        self.snippet_list.pack(fill=tk.X, pady=(0, 4))
        self.snippet_list.configure(bg="#ffffff", relief=tk.FLAT, highlightthickness=1, highlightbackground="#d7dce3")
        self.snippet_list.bind("<Double-Button-1>", self._on_snippet_double_click)

        snippet_btns = ttk.Frame(self.snippet_body)
        snippet_btns.pack(fill=tk.X)
        ttk.Button(snippet_btns, text="Save Current", command=self._save_current_snippet).pack(side=tk.LEFT)
        ttk.Button(snippet_btns, text="Use Selected", command=self._use_selected_snippet).pack(side=tk.LEFT, padx=6)
        ttk.Button(snippet_btns, text="Run Selected", command=self._run_selected_snippet).pack(side=tk.LEFT)
        ttk.Button(snippet_btns, text="Delete Selected", command=self._delete_selected_snippet).pack(side=tk.LEFT, padx=6)
        self._refresh_snippet_list()
        self._toggle_snippets_visibility()

        ai_frame = ttk.LabelFrame(query_frame, text="AI Assistant (Claude)")
        ai_frame.pack(fill=tk.X, pady=(2, 8))

        ttk.Checkbutton(
            ai_frame,
            text="Show AI Assistant",
            variable=self.ai_expanded_var,
            command=self._toggle_ai_visibility,
        ).pack(anchor=tk.W, padx=8, pady=(6, 0))

        self.ai_body = ttk.Frame(ai_frame)
        self.ai_body.pack(fill=tk.X, padx=8, pady=(4, 6))

        model_row = ttk.Frame(self.ai_body)
        model_row.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(model_row, text="Model:").pack(side=tk.LEFT)
        ttk.Entry(model_row, textvariable=self.claude_model_var).pack(
            side=tk.LEFT,
            fill=tk.X,
            expand=True,
            padx=(6, 0),
        )

        key_row = ttk.Frame(self.ai_body)
        key_row.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(key_row, text="API Key:").pack(side=tk.LEFT)
        ttk.Entry(key_row, textvariable=self.claude_api_key_var, show="*").pack(
            side=tk.LEFT,
            fill=tk.X,
            expand=True,
            padx=(6, 0),
        )

        self.ai_prompt_entry = tk.Text(self.ai_body, height=3, wrap=tk.WORD)
        self.ai_prompt_entry.pack(fill=tk.X, pady=(0, 4))
        self.ai_prompt_entry.configure(bg="#ffffff", relief=tk.FLAT, padx=8, pady=6, font=("Segoe UI", 10))
        self.ai_prompt_entry.insert("1.0", "Show top 10 users by score")

        ai_btns = ttk.Frame(self.ai_body)
        ai_btns.pack(fill=tk.X)
        self.ai_generate_btn = ttk.Button(ai_btns, text="Generate SQL (Safe)", command=self.ai_generate_sql)
        self.ai_generate_btn.pack(side=tk.LEFT)
        self.ai_run_btn = ttk.Button(ai_btns, text="Generate + Run", command=self.ai_generate_and_run)
        self.ai_run_btn.pack(side=tk.LEFT, padx=6)
        self._toggle_ai_visibility()

        ttk.Label(query_frame, text="Results", style="Header.TLabel").pack(anchor=tk.W)
        results_wrap = ttk.Frame(query_frame)
        results_wrap.pack(fill=tk.BOTH, expand=True)

        self.result_tree = ttk.Treeview(results_wrap, show="headings")
        self.result_y = ttk.Scrollbar(results_wrap, orient=tk.VERTICAL, command=self.result_tree.yview)
        self.result_x = ttk.Scrollbar(results_wrap, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=self.result_y.set, xscrollcommand=self.result_x.set)

        self.result_tree.grid(row=0, column=0, sticky="nsew")
        self.result_y.grid(row=0, column=1, sticky="ns")
        self.result_x.grid(row=1, column=0, sticky="ew")
        results_wrap.columnconfigure(0, weight=1)
        results_wrap.rowconfigure(0, weight=1)

        ttk.Label(query_frame, text="Messages", style="Header.TLabel").pack(anchor=tk.W, pady=(8, 0))
        self.output = tk.Text(query_frame, wrap=tk.WORD, height=5)
        self.output.pack(fill=tk.X)
        self.output.configure(bg="#ffffff", relief=tk.FLAT, padx=8, pady=6, font=("Consolas", 10))
        self.output.tag_configure("INFO", foreground="#1f2937")
        self.output.tag_configure("ERROR", foreground="#b91c1c")
        self.output.tag_configure("WARN", foreground="#92400e")

    def _browse(self) -> None:
        path = filedialog.askopenfilename(
            title="Select TinyDB file",
            filetypes=[("TinyDB", "*.db"), ("All files", "*.*")],
        )
        if path:
            self.db_path_var.set(path)
            self.open_db(path)

    def _create_new_db(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Create TinyDB file",
            defaultextension=".db",
            filetypes=[("TinyDB", "*.db"), ("All files", "*.*")],
        )
        if not path:
            return
        self.open_db(path)

    def open_db(self, path: str) -> None:
        if not path:
            return
        try:
            if self.db is not None:
                self.db.close()
            self.db = TinyDB(path)
            self.db_path_var.set(path)
            self._save_config()
            self._print_output(f"Opened: {path}", level="INFO")
            self.refresh_metadata()
        except Exception as exc:
            self._log_exception("Open DB failed", exc)
            messagebox.showerror("Open DB Failed", str(exc))

    def refresh_metadata(self) -> None:
        if self.db is None:
            return
        self.table_list.delete(0, tk.END)
        schemas = self.db.executor.schemas
        for table_name in sorted(table.name for table in schemas.values()):
            self.table_list.insert(tk.END, table_name)
        self._set_schema_text("Select a table to view schema")

    def _on_table_select(self, _event: Any) -> None:
        if self.db is None:
            return
        table_name = self._selected_table_name()
        if table_name is None:
            return
        schema = self.db.executor.schemas.get(table_name.lower())
        if schema is None:
            return

        lines = []
        for col in schema.columns:
            suffix = []
            if col.primary_key:
                suffix.append("PRIMARY KEY")
            if col.not_null and not col.primary_key:
                suffix.append("NOT NULL")
            tail = f" {' '.join(suffix)}" if suffix else ""
            lines.append(f"- {col.name}: {col.data_type}{tail}")
        self._set_schema_text("\n".join(lines))

    def _on_table_double_click(self, _event: Any) -> None:
        if self.db is None:
            return
        table_name = self._selected_table_name()
        if table_name is None:
            return
        self._open_table_view(table_name)

    def _on_table_right_click(self, event: tk.Event[tk.Listbox]) -> str:
        idx = self.table_list.nearest(event.y)
        if idx < 0:
            return "break"

        self.table_list.selection_clear(0, tk.END)
        self.table_list.selection_set(idx)
        self.table_list.activate(idx)
        self._on_table_select(None)

        try:
            self.table_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.table_menu.grab_release()
        return "break"

    def _rename_selected_table(self) -> None:
        if self.db is None:
            messagebox.showwarning("Rename Table", "Open a database first.")
            return

        old_name = self._selected_table_name()
        if old_name is None:
            messagebox.showinfo("Rename Table", "Select a table first.")
            return

        new_name = simpledialog.askstring(
            "Rename Table",
            f"Rename '{old_name}' to:",
            initialvalue=old_name,
            parent=self.root,
        )
        if new_name is None:
            return

        clean_name = new_name.strip()
        if not clean_name:
            messagebox.showerror("Rename Table", "Table name cannot be empty.")
            return
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", clean_name):
            messagebox.showerror("Rename Table", "Table name must be a valid identifier (letters, digits, underscore).")
            return
        if clean_name.lower() == old_name.lower():
            return

        try:
            self.db.execute(f"ALTER TABLE {old_name} RENAME TO {clean_name}")
            self.refresh_metadata()
            self._select_table(clean_name)
            self._print_output(f"Renamed table '{old_name}' to '{clean_name}'", level="INFO")
        except Exception as exc:
            self._log_exception("Rename table failed", exc)
            messagebox.showerror("Rename Table Failed", str(exc))

    def _selected_table_name(self) -> str | None:
        selection = self.table_list.curselection()
        if not selection:
            return None
        return self.table_list.get(selection[0])

    def _select_table(self, table_name: str) -> None:
        target = table_name.lower()
        for idx in range(self.table_list.size()):
            current = str(self.table_list.get(idx))
            if current.lower() == target:
                self.table_list.selection_clear(0, tk.END)
                self.table_list.selection_set(idx)
                self.table_list.activate(idx)
                self._on_table_select(None)
                break

    def _open_table_view(self, table_name: str) -> None:
        if self.db is None:
            return

        schema = self.db.executor.schemas.get(table_name.lower())
        if schema is None:
            return

        columns = [col.name for col in schema.columns]

        window = tk.Toplevel(self.root)
        window.title(f"Table: {table_name}")
        window.geometry("900x460")
        window.configure(bg="#eef2f7")

        frame = ttk.Frame(window, padding=8)
        frame.pack(fill=tk.BOTH, expand=True)

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Button(controls, text="Refresh", command=lambda: reload_rows()).pack(side=tk.LEFT)
        ttk.Button(controls, text="Add Row", command=lambda: add_row()).pack(side=tk.LEFT, padx=6)
        ttk.Button(controls, text="Edit Selected Row", command=lambda: edit_selected_row()).pack(
            side=tk.LEFT,
            padx=6,
        )
        ttk.Button(controls, text="Delete Selected Row", command=lambda: delete_selected_row()).pack(side=tk.LEFT)
        ttk.Label(controls, text="Filter:").pack(side=tk.LEFT, padx=(12, 4))
        filter_var = tk.StringVar()
        filter_entry = ttk.Entry(controls, textvariable=filter_var, width=24)
        filter_entry.pack(side=tk.LEFT)
        ttk.Button(controls, text="Clear", command=lambda: filter_var.set("")).pack(side=tk.LEFT, padx=(4, 0))

        tree = ttk.Treeview(frame, columns=columns, show="headings")
        y_scroll = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
        x_scroll = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        rows_by_item: dict[str, dict[str, Any]] = {}
        all_rows: list[dict[str, Any]] = []
        sort_column: str | None = None
        sort_desc = False
        status_var = tk.StringVar(value="(0 rows)")

        def _sort_key(row: dict[str, Any], column: str) -> tuple[int, str]:
            value = row.get(column)
            if value is None:
                return (1, "")
            return (0, str(value).lower())

        def _refresh_headings() -> None:
            for heading_col in columns:
                marker = ""
                if heading_col == sort_column:
                    marker = " ▼" if sort_desc else " ▲"
                tree.heading(
                    heading_col,
                    text=f"{heading_col}{marker}",
                    command=lambda c=heading_col: on_sort(c),
                )

        def _render_rows(rows: list[dict[str, Any]]) -> None:
            rows_by_item.clear()
            tree.delete(*tree.get_children())
            for row in rows:
                item_id = tree.insert("", tk.END, values=[_scalar(row.get(col)) for col in columns])
                rows_by_item[item_id] = row
            if filter_var.get().strip():
                status_var.set(f"{len(rows)} shown / {len(all_rows)} total")
            else:
                status_var.set(f"{len(rows)} row(s)")

        def apply_filter_and_sort() -> None:
            query = filter_var.get().strip().lower()
            filtered_rows = all_rows
            if query:
                filtered_rows = [
                    row
                    for row in all_rows
                    if any(query in str(row.get(col, "")).lower() for col in columns)
                ]

            if sort_column is not None:
                filtered_rows = sorted(
                    filtered_rows,
                    key=lambda row: _sort_key(row, sort_column),
                    reverse=sort_desc,
                )
            _render_rows(filtered_rows)

        def on_sort(column: str) -> None:
            nonlocal sort_column, sort_desc
            if sort_column == column:
                sort_desc = not sort_desc
            else:
                sort_column = column
                sort_desc = False
            _refresh_headings()
            apply_filter_and_sort()

        for col in columns:
            tree.heading(col, text=col, command=lambda c=col: on_sort(c))
            tree.column(col, width=140, anchor=tk.W, stretch=True)

        def reload_rows() -> None:
            nonlocal all_rows
            all_rows = self.db.execute(f"SELECT * FROM {table_name}")
            apply_filter_and_sort()

        filter_var.trace_add("write", lambda *_: apply_filter_and_sort())
        _refresh_headings()

        def edit_selected_row() -> None:
            selection = tree.selection()
            if not selection:
                messagebox.showinfo("Edit Row", "Select a row first.")
                return

            item_id = selection[0]
            original = rows_by_item.get(item_id)
            if original is None:
                return

            pk_col = schema.pk_column
            if pk_col is None:
                messagebox.showwarning("Edit Row", "Editing requires a PRIMARY KEY table.")
                return

            dialog = tk.Toplevel(window)
            dialog.title(f"Edit Row - {table_name}")
            dialog.geometry("460x420")
            dialog.transient(window)
            dialog.grab_set()

            body = ttk.Frame(dialog, padding=10)
            body.pack(fill=tk.BOTH, expand=True)

            entries: dict[str, tk.Entry] = {}
            for row_idx, col in enumerate(schema.columns):
                ttk.Label(body, text=f"{col.name} ({col.data_type})").grid(
                    row=row_idx,
                    column=0,
                    sticky="w",
                    padx=(0, 8),
                    pady=4,
                )
                entry = ttk.Entry(body)
                entry.grid(row=row_idx, column=1, sticky="ew", pady=4)
                value = original.get(col.name)
                entry.insert(0, "NULL" if value is None else str(value))
                if col.primary_key:
                    entry.configure(state="disabled")
                entries[col.name] = entry

            body.columnconfigure(1, weight=1)

            def save() -> None:
                try:
                    assignments: list[str] = []
                    for col in schema.columns:
                        if col.primary_key:
                            continue
                        typed_value = _parse_editor_value(col.data_type, entries[col.name].get())
                        assignments.append(f"{col.name} = {_to_sql_literal(typed_value)}")

                    if not assignments:
                        dialog.destroy()
                        return

                    pk_value = _to_sql_literal(original.get(pk_col.name))
                    sql = f"UPDATE {table_name} SET {', '.join(assignments)} WHERE {pk_col.name} = {pk_value}"
                    self.db.execute(sql)
                    reload_rows()
                    self.refresh_metadata()
                    dialog.destroy()
                except Exception as exc:
                    messagebox.showerror("Edit Row Failed", str(exc), parent=dialog)

            actions = ttk.Frame(body)
            actions.grid(row=len(schema.columns), column=0, columnspan=2, sticky="e", pady=(10, 0))
            ttk.Button(actions, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT)
            ttk.Button(actions, text="Save", command=save).pack(side=tk.RIGHT, padx=(0, 6))

        def add_row() -> None:
            dialog = tk.Toplevel(window)
            dialog.title(f"Add Row - {table_name}")
            dialog.geometry("460x420")
            dialog.transient(window)
            dialog.grab_set()

            body = ttk.Frame(dialog, padding=10)
            body.pack(fill=tk.BOTH, expand=True)

            entries: dict[str, tk.Entry] = {}
            for row_idx, col in enumerate(schema.columns):
                ttk.Label(body, text=f"{col.name} ({col.data_type})").grid(
                    row=row_idx,
                    column=0,
                    sticky="w",
                    padx=(0, 8),
                    pady=4,
                )
                entry = ttk.Entry(body)
                entry.grid(row=row_idx, column=1, sticky="ew", pady=4)
                entries[col.name] = entry

            body.columnconfigure(1, weight=1)

            pk_col = schema.pk_column
            if pk_col is not None and pk_col.data_type == "INTEGER":
                try:
                    current_rows = self.db.execute(f"SELECT * FROM {table_name}")
                    max_pk = max(
                        (int(row.get(pk_col.name)) for row in current_rows if row.get(pk_col.name) is not None),
                        default=0,
                    )
                    entries[pk_col.name].insert(0, str(max_pk + 1))
                except Exception:
                    pass

            def save_new() -> None:
                try:
                    ordered_values: list[str] = []
                    for col in schema.columns:
                        raw_text = entries[col.name].get().strip()
                        if raw_text == "":
                            typed_value = None
                        else:
                            typed_value = _parse_editor_value(col.data_type, raw_text)
                        ordered_values.append(_to_sql_literal(typed_value))

                    sql = f"INSERT INTO {table_name} VALUES ({', '.join(ordered_values)})"
                    self.db.execute(sql)
                    reload_rows()
                    self.refresh_metadata()
                    dialog.destroy()
                except Exception as exc:
                    messagebox.showerror("Add Row Failed", str(exc), parent=dialog)

            actions = ttk.Frame(body)
            actions.grid(row=len(schema.columns), column=0, columnspan=2, sticky="e", pady=(10, 0))
            ttk.Button(actions, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT)
            ttk.Button(actions, text="Insert", command=save_new).pack(side=tk.RIGHT, padx=(0, 6))

        def delete_selected_row() -> None:
            selection = tree.selection()
            if not selection:
                messagebox.showinfo("Delete Row", "Select a row first.")
                return

            pk_col = schema.pk_column
            if pk_col is None:
                messagebox.showwarning("Delete Row", "Deleting requires a PRIMARY KEY table.")
                return

            item_id = selection[0]
            original = rows_by_item.get(item_id)
            if original is None:
                return

            should_delete = messagebox.askyesno(
                "Delete Row",
                f"Delete selected row where {pk_col.name} = {original.get(pk_col.name)}?",
                parent=window,
            )
            if not should_delete:
                return

            try:
                pk_value = _to_sql_literal(original.get(pk_col.name))
                self.db.execute(f"DELETE FROM {table_name} WHERE {pk_col.name} = {pk_value}")
                reload_rows()
                self.refresh_metadata()
            except Exception as exc:
                messagebox.showerror("Delete Row Failed", str(exc), parent=window)

        reload_rows()

        tree.grid(row=1, column=0, sticky="nsew")
        y_scroll.grid(row=1, column=1, sticky="ns")
        x_scroll.grid(row=2, column=0, sticky="ew")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        ttk.Label(frame, textvariable=status_var).grid(row=3, column=0, sticky="w", pady=(6, 0))

    def run_sql(self) -> None:
        if self.db is None:
            messagebox.showwarning("No DB Open", "Open a database first.")
            return
        self._hide_autocomplete()
        sql = self.query_entry.get("1.0", tk.END).strip()
        if not sql:
            return

        self._execute_sql_text(sql)

    def ai_generate_sql(self) -> None:
        self._start_ai_generation(run_after=False)

    def ai_generate_and_run(self) -> None:
        self._start_ai_generation(run_after=True)

    def _start_ai_generation(self, run_after: bool) -> None:
        if self.ai_request_inflight:
            self._print_output("AI request already running. Please wait.", level="WARN")
            return

        if self.db is None:
            messagebox.showwarning("AI Assistant", "Open a database first.")
            return

        api_key = self.claude_api_key_var.get().strip()
        if not api_key:
            messagebox.showwarning("AI Assistant", "Enter your Claude API key first.")
            return

        prompt = self.ai_prompt_entry.get("1.0", tk.END).strip()
        if not prompt:
            messagebox.showwarning("AI Assistant", "Enter a prompt for the AI assistant.")
            return

        self.ai_request_inflight = True
        self._set_ai_buttons_enabled(False)
        self._print_output("Sending request to Claude...", level="INFO")

        def worker() -> None:
            try:
                raw = self._call_claude(prompt=prompt, api_key=api_key)
                sql = self._extract_sql(raw)
                if not sql:
                    raise ValueError("Claude response did not include SQL")
                try:
                    self.root.after(0, lambda: self._on_ai_success(sql, run_after))
                except tk.TclError:
                    return
            except Exception as exc:
                try:
                    self.root.after(0, lambda err=exc: self._on_ai_failure(err))
                except tk.TclError:
                    return

        threading.Thread(target=worker, daemon=True).start()

    def _on_ai_success(self, sql: str, run_after: bool) -> None:
        self.ai_request_inflight = False
        self._set_ai_buttons_enabled(True)
        self._set_query_text(sql)
        self._print_output(f"AI generated SQL: {sql}", level="INFO")

        if not run_after:
            self._print_output("Review/edit, then click Run SQL.", level="INFO")
            return

        if self._is_mutating_sql(sql):
            should_run = messagebox.askyesno(
                "Confirm AI Query",
                "AI generated a mutating query. Do you want to run it?\n\n"
                f"{sql}",
            )
            if not should_run:
                self._print_output("AI query generated but not executed.", level="WARN")
                return
        self._execute_sql_text(sql)

    def _on_ai_failure(self, exc: Exception) -> None:
        self.ai_request_inflight = False
        self._set_ai_buttons_enabled(True)
        self._log_exception("AI generation failed", exc)
        self._print_output(f"AI error: {exc}. See log file for details.", level="ERROR")
        messagebox.showerror("AI Generation Failed", str(exc))

    def _set_ai_buttons_enabled(self, enabled: bool) -> None:
        state = tk.NORMAL if enabled else tk.DISABLED
        self.ai_generate_btn.configure(state=state)
        self.ai_run_btn.configure(state=state)

    def _generate_sql_from_ai_prompt(self) -> str | None:
        if self.db is None:
            messagebox.showwarning("AI Assistant", "Open a database first.")
            return None

        api_key = self.claude_api_key_var.get().strip()
        if not api_key:
            messagebox.showwarning("AI Assistant", "Enter your Claude API key first.")
            return None

        prompt = self.ai_prompt_entry.get("1.0", tk.END).strip()
        if not prompt:
            messagebox.showwarning("AI Assistant", "Enter a prompt for the AI assistant.")
            return None

        try:
            self._print_output("Sending request to Claude...", level="INFO")
            raw = self._call_claude(prompt=prompt, api_key=api_key)
            sql = self._extract_sql(raw)
            if not sql:
                raise ValueError("Claude response did not include SQL")
            self._print_output(f"AI generated SQL: {sql}", level="INFO")
            return sql
        except Exception as exc:
            self._log_exception("AI generation failed", exc)
            self._print_output(f"AI error: {exc}. See log file for details.", level="ERROR")
            return None

    def _call_claude(self, prompt: str, api_key: str) -> str:
        model_name = self.claude_model_var.get().strip() or CLAUDE_MODEL

        schema_lines: list[str] = []
        data_lines: list[str] = []
        if self.db is not None:
            for table in sorted(self.db.executor.schemas.values(), key=lambda item: item.name.lower()):
                cols = ", ".join(f"{col.name}:{col.data_type}" for col in table.columns)
                schema_lines.append(f"- {table.name}({cols})")
                try:
                    sample_rows = self.db.execute(f"SELECT * FROM {table.name} LIMIT {AI_SAMPLE_ROW_LIMIT}")
                    data_lines.append(
                        f"- {table.name}: {json.dumps(sample_rows, ensure_ascii=True)}"
                    )
                except Exception as exc:
                    data_lines.append(f"- {table.name}: <sample unavailable: {exc}>")
        schema_context = "\n".join(schema_lines) if schema_lines else "- (no tables)"
        data_context = "\n".join(data_lines) if data_lines else "- (no sample rows)"

        payload = {
            "model": model_name,
            "max_tokens": 400,
            "system": CLAUDE_SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Current schema:\n"
                        f"{schema_context}\n\n"
                        "Sample rows (read-only context, may be partial):\n"
                        f"{data_context}\n\n"
                        "Task:\n"
                        f"{prompt}"
                    ),
                }
            ],
        }

        request = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code == 404 and "model" in detail.lower():
                raise ValueError(
                    f"Claude model not found: '{model_name}'. "
                    "Update the Model field in AI Assistant to one enabled on your account. "
                    f"API detail: {detail}"
                ) from exc
            raise ValueError(f"Claude API error ({exc.code}): {detail}") from exc
        except urllib.error.URLError as exc:
            raise ValueError(f"Network error contacting Claude API: {exc.reason}") from exc

        data = json.loads(body)
        parts = data.get("content", [])
        texts: list[str] = []
        for part in parts:
            if isinstance(part, dict) and part.get("type") == "text":
                text = part.get("text")
                if isinstance(text, str):
                    texts.append(text)
        return "\n".join(texts).strip()

    def _extract_sql(self, text: str) -> str:
        candidate = text.strip()
        if candidate.startswith("```"):
            lines = candidate.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            candidate = "\n".join(lines).strip()
        if candidate.endswith(";"):
            return candidate
        return f"{candidate};"

    def _set_query_text(self, sql: str) -> None:
        self.query_entry.delete("1.0", tk.END)
        self.query_entry.insert("1.0", sql)
        self._highlight_sql()

    def _is_mutating_sql(self, sql: str) -> bool:
        stripped = sql.strip().lstrip("(")
        if not stripped:
            return False
        first = stripped.split(None, 1)[0].upper()
        return first in {"INSERT", "UPDATE", "DELETE", "ALTER", "DROP", "CREATE"}

    def _execute_sql_text(self, sql: str) -> None:
        try:
            self._record_query_history(sql)
            self._print_output(f"Running SQL: {sql}", level="INFO")
            result = self.db.execute(sql)
            if isinstance(result, list) and (not result or isinstance(result[0], dict)):
                self._show_result_rows(result)
                self._print_output(f"Query succeeded: {len(result)} row(s)", level="INFO")
            else:
                self._clear_result_rows()
                self._print_output(f"Command result: {result}", level="INFO")
            self.refresh_metadata()
        except Exception as exc:
            self._clear_result_rows()
            self._log_exception("SQL execution failed", exc)
            msg = str(exc)
            if isinstance(exc, ParseError) and msg == "Unsupported SQL syntax":
                msg = (
                    "Unsupported SQL syntax. Hint: column/table names cannot contain spaces. "
                    "Use exact schema identifiers like player_id, coin_side, bet_amount."
                )
            self._print_output(f"SQL error: {msg}. See log file for traceback.", level="ERROR")
            messagebox.showerror("SQL Execution Failed", msg)

    def _set_schema_text(self, text: str) -> None:
        self.schema_text.configure(state=tk.NORMAL)
        self.schema_text.delete("1.0", tk.END)
        self.schema_text.insert("1.0", text)
        self.schema_text.configure(state=tk.DISABLED)

    def _print_output(self, text: str, level: str = "INFO") -> None:
        tag = level if level in {"INFO", "ERROR", "WARN"} else "INFO"
        self.output.insert(tk.END, f"[{tag}] {text}\n", tag)
        self.output.see(tk.END)

    def _log_exception(self, context: str, exc: Exception) -> None:
        detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.logger.error("%s: %s\n%s", context, exc, detail)

    def _clear_result_rows(self) -> None:
        self.result_tree.delete(*self.result_tree.get_children())
        self.result_tree["columns"] = ()

    def _show_result_rows(self, rows: list[dict[str, Any]]) -> None:
        self._clear_result_rows()
        if not rows:
            return

        columns = list(rows[0].keys())
        self.result_tree["columns"] = columns
        for col in columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=140, anchor=tk.W, stretch=True)
        for row in rows:
            self.result_tree.insert("", tk.END, values=[_scalar(row.get(col)) for col in columns])

    def _highlight_sql(self) -> None:
        text = self.query_entry.get("1.0", "end-1c")
        for tag in ("sql_keyword", "sql_type", "sql_string", "sql_number"):
            self.query_entry.tag_remove(tag, "1.0", tk.END)

        for match in re.finditer(r"'[^']*'", text):
            self.query_entry.tag_add(
                "sql_string",
                f"1.0+{match.start()}c",
                f"1.0+{match.end()}c",
            )
        for match in re.finditer(r"\b\d+(?:\.\d+)?\b", text):
            self.query_entry.tag_add(
                "sql_number",
                f"1.0+{match.start()}c",
                f"1.0+{match.end()}c",
            )
        for match in re.finditer(r"\b[A-Za-z_][A-Za-z0-9_]*\b", text):
            word = match.group(0).upper()
            tag = None
            if word in SQL_TYPES:
                tag = "sql_type"
            elif word in SQL_KEYWORDS:
                tag = "sql_keyword"
            if tag is not None:
                self.query_entry.tag_add(
                    tag,
                    f"1.0+{match.start()}c",
                    f"1.0+{match.end()}c",
                )

    def _on_query_key_release(self, event: tk.Event[tk.Text]) -> None:
        if event.keysym in {"Up", "Down", "Tab", "Escape", "Shift_L", "Shift_R", "Control_L", "Control_R", "Alt_L", "Alt_R"}:
            return
        self._highlight_sql()
        self._refresh_autocomplete()

    def _on_query_tab(self, _event: tk.Event[tk.Text]) -> str:
        if self.autocomplete_popup is None:
            self._refresh_autocomplete()
        if self.autocomplete_popup is not None:
            self._accept_autocomplete()
            return "break"
        self.query_entry.insert("insert", "    ")
        return "break"

    def _on_query_down(self, _event: tk.Event[tk.Text]) -> str | None:
        if self.autocomplete_popup is None or self.autocomplete_list is None:
            return None
        size = self.autocomplete_list.size()
        if size == 0:
            return "break"
        selected = self.autocomplete_list.curselection()
        idx = selected[0] if selected else -1
        idx = min(size - 1, idx + 1)
        self.autocomplete_list.selection_clear(0, tk.END)
        self.autocomplete_list.selection_set(idx)
        self.autocomplete_list.activate(idx)
        return "break"

    def _on_query_up(self, _event: tk.Event[tk.Text]) -> str | None:
        if self.autocomplete_popup is None or self.autocomplete_list is None:
            return None
        size = self.autocomplete_list.size()
        if size == 0:
            return "break"
        selected = self.autocomplete_list.curselection()
        idx = selected[0] if selected else size
        idx = max(0, idx - 1)
        self.autocomplete_list.selection_clear(0, tk.END)
        self.autocomplete_list.selection_set(idx)
        self.autocomplete_list.activate(idx)
        return "break"

    def _on_query_escape(self, _event: tk.Event[tk.Text]) -> str | None:
        if self.autocomplete_popup is None:
            return None
        self._hide_autocomplete()
        return "break"

    def _on_query_click(self, _event: tk.Event[tk.Text]) -> None:
        self._hide_autocomplete()

    def _autocomplete_word_bounds(self) -> tuple[str, str, str] | None:
        cursor = self.query_entry.index("insert")
        line_start = self.query_entry.index(f"{cursor} linestart")
        line_end = self.query_entry.index(f"{cursor} lineend")
        left = self.query_entry.get(line_start, cursor)
        right = self.query_entry.get(cursor, line_end)

        left_match = re.search(r"[A-Za-z_][A-Za-z0-9_]*$", left)
        if left_match is None:
            return None

        right_match = re.match(r"[A-Za-z0-9_]*", right)
        right_len = len(right_match.group(0)) if right_match is not None else 0
        prefix = left_match.group(0)
        start = self.query_entry.index(f"{cursor}-{len(prefix)}c")
        end = self.query_entry.index(f"{cursor}+{right_len}c")
        return start, end, prefix

    def _autocomplete_terms(self) -> list[str]:
        terms = set(SQL_KEYWORDS) | set(SQL_TYPES)
        if self.db is not None:
            for schema in self.db.executor.schemas.values():
                terms.add(schema.name)
                for col in schema.columns:
                    terms.add(col.name)
        return sorted(terms, key=lambda value: value.upper())

    def _refresh_autocomplete(self) -> None:
        bounds = self._autocomplete_word_bounds()
        if bounds is None:
            self._hide_autocomplete()
            return
        _start, _end, prefix = bounds
        prefix_upper = prefix.upper()
        if len(prefix_upper) < 2:
            self._hide_autocomplete()
            return

        options = [term for term in self._autocomplete_terms() if term.upper().startswith(prefix_upper)]
        options = options[:8]
        if not options:
            self._hide_autocomplete()
            return
        self._show_autocomplete(options)

    def _show_autocomplete(self, options: list[str]) -> None:
        if self.autocomplete_popup is None or self.autocomplete_list is None:
            popup = tk.Toplevel(self.root)
            popup.withdraw()
            popup.overrideredirect(True)
            popup.transient(self.root)
            popup.configure(bg="#2a2f3a")

            listbox = tk.Listbox(popup, height=min(8, len(options)), activestyle="none", relief=tk.FLAT)
            listbox.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)
            listbox.configure(
                bg="#1f2430",
                fg="#f2f5fb",
                selectbackground="#3b82f6",
                selectforeground="#ffffff",
                font=("Consolas", 10),
            )
            listbox.bind("<ButtonRelease-1>", lambda _e: self._accept_autocomplete())

            self.autocomplete_popup = popup
            self.autocomplete_list = listbox

        assert self.autocomplete_popup is not None
        assert self.autocomplete_list is not None

        self.autocomplete_list.delete(0, tk.END)
        for option in options:
            self.autocomplete_list.insert(tk.END, option)
        self.autocomplete_list.selection_set(0)
        self.autocomplete_list.activate(0)

        bbox = self.query_entry.bbox("insert")
        if bbox is None:
            self._hide_autocomplete()
            return
        x, y, _w, h = bbox
        popup_x = self.query_entry.winfo_rootx() + x
        popup_y = self.query_entry.winfo_rooty() + y + h + 2
        self.autocomplete_popup.geometry(f"260x{min(180, 26 * max(1, len(options)))}+{popup_x}+{popup_y}")
        self.autocomplete_popup.deiconify()
        self.autocomplete_popup.lift()

    def _accept_autocomplete(self) -> None:
        if self.autocomplete_popup is None or self.autocomplete_list is None:
            return
        bounds = self._autocomplete_word_bounds()
        selection = self.autocomplete_list.curselection()
        if bounds is None or not selection:
            self._hide_autocomplete()
            return

        start, end, _prefix = bounds
        value = self.autocomplete_list.get(selection[0])
        self.query_entry.delete(start, end)
        self.query_entry.insert(start, value)
        self.query_entry.mark_set("insert", f"{start}+{len(value)}c")
        self._hide_autocomplete()
        self._highlight_sql()

    def _hide_autocomplete(self) -> None:
        if self.autocomplete_popup is not None:
            self.autocomplete_popup.withdraw()


def main() -> None:
    parser = argparse.ArgumentParser(description="TinyDB GUI viewer")
    parser.add_argument("db_path", nargs="?", help="Optional path to .db file")
    args = parser.parse_args()

    root = tk.Tk()
    app = TinyDBGui(root, args.db_path)

    def on_close() -> None:
        app._save_config()
        if app.db is not None:
            app.db.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()

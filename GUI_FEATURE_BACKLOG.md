# TinyDB GUI Feature Backlog

## 1) SQL History Panel
- Keep a list of recently executed SQL statements.
- Click an item to load it back into the SQL editor.
- Optional: one-click re-run button.

## 2) Saved Query Snippets
- Let users pin/favorite useful queries.
- Save snippets locally (same style as GUI config persistence).
- Add quick insert/execute controls.

## 3) Full Row CRUD in Table Viewer
- Add **Add Row** dialog.
- Add **Delete Selected Row** with confirmation.
- Keep current **Edit Selected Row** flow.

## 4) Inline Table Filter + Sort
- Per-table quick filter input (e.g. "contains").
- Clickable column headers to toggle ASC/DESC sort.
- Keep filtering/sorting responsive for common table sizes.

## 5) Schema Actions in GUI
- Dialogs/buttons for:
  - Add column
  - Remove last column
  - Rename column
  - Drop table (with strong confirmation)
- Surface clear limitations from engine rules.

## 6) AI "Explain This SQL"
- New AI action to explain currently typed SQL.
- Include expected effect and potential risks.
- Keep result concise and actionable.

## 7) AI Guardrails Toggle
- Optional modes:
  - Read-only (SELECT only)
  - Block destructive statements (DROP/DELETE)
- Enforce guardrails before execution.

## 8) Richer Error Diagnostics
- Show parser location/snippet in a dedicated diagnostics section.
- Offer likely fixes (identifier mismatch, unsupported syntax, etc.).
- Add one-click copy for full error details.

## 9) CSV Import/Export
- Export query/table results to CSV.
- Import CSV into selected table with column mapping and validation.

## 10) Status Bar + DB Stats
- Show open DB path, table count, and query timing.
- Optional lightweight row-count info per table.
- Improve visibility of current app state.

## Suggested Implementation Order
1. SQL History Panel
2. Full Row CRUD in Table Viewer
3. Inline Table Filter + Sort
4. Schema Actions in GUI
5. Richer Error Diagnostics
6. Saved Query Snippets
7. CSV Import/Export
8. AI Guardrails Toggle
9. AI Explain This SQL
10. Status Bar + DB Stats

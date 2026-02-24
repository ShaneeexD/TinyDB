@echo off
setlocal

if not exist ".venv\Scripts\python.exe" (
  echo [TinyDB GUI] .venv not found. Run install_gui_deps.bat first.
  exit /b 1
)

echo [TinyDB GUI] Launching GUI...
call .venv\Scripts\python.exe -m tinydb_engine.gui %*

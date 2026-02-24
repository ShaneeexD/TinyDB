@echo off
setlocal

echo [TinyDB] Setting up virtual environment...
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
)

echo [TinyDB] Upgrading pip...
call .venv\Scripts\python.exe -m pip install -U pip
if errorlevel 1 goto :fail

echo [TinyDB] Installing tinydb_engine (editable)...
call .venv\Scripts\python.exe -m pip install -e .
if errorlevel 1 goto :fail

echo.
echo [TinyDB] Install complete.
echo Use: .venv\Scripts\python -m tinydb_engine.gui
echo Or:  .venv\Scripts\tinydb-gui
goto :eof

:fail
echo.
echo [TinyDB] Install failed.
exit /b 1

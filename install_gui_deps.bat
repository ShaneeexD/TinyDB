@echo off
setlocal

echo [TinyDB GUI] Preparing environment...
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
)

echo [TinyDB GUI] Upgrading pip...
call .venv\Scripts\python.exe -m pip install -U pip
if errorlevel 1 goto :fail

echo [TinyDB GUI] Installing tinydb_engine for GUI use...
call .venv\Scripts\python.exe -m pip install -e .
if errorlevel 1 goto :fail

echo.
echo [TinyDB GUI] Ready.
echo Run GUI with: run_gui.bat [optional_db_path]
goto :eof

:fail
echo.
echo [TinyDB GUI] Setup failed.
exit /b 1

@echo off
cd /d "%~dp0.."
rem Task Scheduler runs with no console attached, so Python falls back to the
rem system ANSI codepage (cp1252) for stdout/stderr and crashes on the
rem checkmark characters generate_journal.py prints. Force UTF-8 explicitly.
set PYTHONIOENCODING=utf-8
"venv\Scripts\python.exe" backtesting\generate_journal.py

@echo off
cd /d "%~dp0.."
"venv\Scripts\python.exe" backtesting\generate_journal.py

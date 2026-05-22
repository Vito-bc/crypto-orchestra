@echo off
cd /d "%~dp0\.."
venv\Scripts\python pipeline\runner.py >> logs\scheduler.log 2>&1

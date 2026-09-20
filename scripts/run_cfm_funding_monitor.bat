@echo off
cd /d "%~dp0.."
rem Task Scheduler has no console attached; make all diagnostic output UTF-8.
set PYTHONIOENCODING=utf-8
"venv\Scripts\python.exe" backtesting\cfm_funding_monitor.py

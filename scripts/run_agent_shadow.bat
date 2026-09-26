@echo off
cd /d "%~dp0.."
rem Task Scheduler has no console attached; make all diagnostic output UTF-8.
set PYTHONIOENCODING=utf-8
"venv\Scripts\python.exe" -m agent_shadow.runner --variant agent-shadow-event-v1
if errorlevel 1 exit /b %errorlevel%
"venv\Scripts\python.exe" -m agent_shadow.attachments

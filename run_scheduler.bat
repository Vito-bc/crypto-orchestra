@echo off
title Crypto Orchestra Scheduler
cd /d "%~dp0"

echo ============================================================
echo  CRYPTO ORCHESTRA — Starting scheduler
echo  Interval: 60 minutes  (pass arg to change, e.g. 30)
echo  Log file: logs\scheduler.log
echo  Press Ctrl+C to stop
echo ============================================================
echo.

venv\Scripts\python pipeline\scheduler.py %1

echo.
echo [Scheduler] Exited. Press any key to close.
pause > nul

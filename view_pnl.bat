@echo off
title Crypto Orchestra — P&L Dashboard
cd /d "%~dp0"
venv\Scripts\python pipeline\dashboard.py %1
echo.
pause

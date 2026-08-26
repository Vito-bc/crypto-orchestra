@echo off
REM One STF execution-cost observation. Read-only: sweeps the PUBLIC order book
REM and places no orders.
REM
REM Scheduled daily at 20:05 America/New_York, which is 00:05 UTC on eastern
REM daylight time and 01:05 UTC on standard time. Both fall inside the
REM protocol's 90-minute execution window, and Task Scheduler follows the
REM local-time change on its own.
REM
REM Deliberately NOT here:
REM   --force   a sample taken outside the window describes a different market.
REM             A missed day stays missed; the probe refuses and exits 2.
REM   pause     nothing may block an unattended run.
REM   secrets   no key material, no paths to key files. The fee tier is opt-in
REM             through STF_FEE_VIEW_ONLY_KEY_FILE in the environment, and
REM             until a view-only key exists "fee_tier: unavailable" is the
REM             expected state, not a failure.
setlocal
cd /d "%~dp0.."

if not exist "logs" mkdir "logs"
set "OPS_LOG=logs\stf_cost_probe_runs.log"

REM Operational log only. The observations themselves go to
REM logs\stf_cost_probe.jsonl, which the probe writes and --report reads.
echo.>> "%OPS_LOG%"
echo ==== %DATE% %TIME% ====>> "%OPS_LOG%"
"venv\Scripts\python.exe" backtesting\stf_cost_probe.py >> "%OPS_LOG%" 2>&1

REM The scheduler decides whether to retry from this code, so pass the probe's
REM own exit status through unchanged.
endlocal & exit /b %ERRORLEVEL%

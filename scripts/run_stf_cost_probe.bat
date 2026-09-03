@echo off
REM One STF execution-cost observation. Read-only: sweeps the PUBLIC order book
REM and places no orders.
REM
REM Scheduled daily at 00:05 UTC -- five minutes into the protocol's 90-minute
REM execution window, year round. That instant displays as 20:05 Eastern
REM daylight time and 19:05 Eastern standard time; the schedule is anchored to
REM the UTC instant, not to either wall-clock reading. See
REM scripts\register_stf_cost_probe_task.ps1 for why that distinction matters.
REM
REM The task runs as the logged-in user with no stored password. WakeToRun is
REM armed, so ordinary standby is not a miss -- Windows wakes the machine for
REM the trigger and lets it sleep again afterwards. A day is still lost if the
REM machine hibernated (an RTC timer cannot resume S4; on battery, modern
REM standby hibernates once the standby budget runs out, so AC is recommended)
REM or if the user signed out. A locked screen is fine.
REM
REM A lost day stays lost: see --force below. Three attempts are made, at
REM +0/+10/+20 minutes, all inside the 90-minute window; they are redundancy
REM for ONE observation, never extra observations.
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

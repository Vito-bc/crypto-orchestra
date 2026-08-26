<#
Registers the daily STF execution-cost observation with Windows Task Scheduler.

Run once, from an ordinary (non-elevated) PowerShell:

    powershell -ExecutionPolicy Bypass -File scripts\register_stf_cost_probe_task.ps1

WHY 20:05 LOCAL AND NOT A UTC TIME
----------------------------------
Task Scheduler triggers on local time and follows the daylight-saving change on
its own. On this machine (Eastern Time) 20:05 local is 00:05 UTC in summer and
01:05 UTC in winter. Both sit inside the protocol's 90-minute execution window,
so the schedule needs no seasonal edit. A fixed UTC time would drift out of the
window twice a year.

WHAT THE SETTINGS MEAN
----------------------
  RestartCount 2 / RestartInterval 10m   two retries, ten minutes apart. Three
                                         attempts still land inside the window.
  MultipleInstances IgnoreNew            never two probes at once; a retry that
                                         overlaps a slow run is dropped.
  StartWhenAvailable = $false            a missed day is NOT made up later. A
                                         late catch-up would sample the wrong
                                         hour, which is the one thing this
                                         measurement cannot tolerate.
  ExecutionTimeLimit 15m                 a hung HTTP call must not sit until
                                         tomorrow holding the instance lock.
  batteries allowed                      a laptop on battery still samples;
                                         otherwise the schedule silently thins.

WakeToRun is deliberately off. If the machine is asleep at 20:05 the day is
lost, and it stays lost — see StartWhenAvailable above.
#>

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "scripts\run_stf_cost_probe.bat"
if (-not (Test-Path $runner)) { throw "runner not found at $runner" }

$action = New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Daily -At "20:05"
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
$settings.StartWhenAvailable = $false

$description = @"
Phase 7R-2: one read-only STF execution-cost observation per day at 20:05
America/New_York (00:05 UTC on daylight time, 01:05 UTC on standard time; both
inside the protocol execution window). Sweeps the PUBLIC order book and places
no orders. A missed day is NOT made up outside the window.
"@

Register-ScheduledTask -TaskName "CryptoOrchestra-STF-CostProbe" `
    -Action $action -Trigger $trigger -Settings $settings `
    -Description $description -Force | Out-Null

Get-ScheduledTaskInfo -TaskName "CryptoOrchestra-STF-CostProbe" |
    Select-Object TaskName, NextRunTime | Format-List

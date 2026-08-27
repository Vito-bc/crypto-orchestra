<#
Registers the daily STF execution-cost observation with Windows Task Scheduler.

Run once, from an ordinary (non-elevated) PowerShell:

    powershell -ExecutionPolicy Bypass -File scripts\register_stf_cost_probe_task.ps1

THE SCHEDULE IS ANCHORED IN UTC, ON PURPOSE
-------------------------------------------
The measurement is defined in UTC: the protocol executes on the first hourly
bar after the daily close, and the probe refuses any sample more than
EXECUTION_WINDOW_MINUTES past 00:00 UTC. So the schedule must hold a UTC
instant fixed, not a wall-clock reading.

An earlier version passed `-At "20:05"`, a LOCAL time. That is not a
local-time rule: Task Scheduler stamped the boundary as 2026-08-26T20:05:00
**-04:00**, and a StartBoundary carrying an offset is an absolute instant,
used regardless of the current time zone or daylight saving. Two consequences,
the second worse than the first:

  * the task would keep firing at 00:05 UTC while DISPLAYING as 19:05 once
    Eastern time left daylight saving — harmless in itself, since the UTC
    instant is what matters;
  * but re-running this script in WINTER would have stamped -05:00, pinning
    the task to 01:05 UTC instead. The actual sampling time would then depend
    on the season in which someone happened to re-register it. Silent, and
    invisible in the report.

So the boundary is computed here from [DateTime]::UtcNow. Task Scheduler then
stores it stamped with the machine's own offset -- a literal Z cannot be kept,
the service rewrites it on registration -- but the INSTANT is now correct by
construction whatever season the script is run in. Winter registration writes
19:05-05:00, which is the same 00:05 UTC as summer's 20:05-04:00.

Because the spelling is not ours to control, the check at the bottom parses the
stored boundary back to UTC and compares the instant. That is the property that
matters, and it is exactly the check that would have caught the defect above.

    00:05 UTC every day  =  20:05 Eastern daylight time
                         =  19:05 Eastern standard time

Five minutes into a ninety-minute window, year round.

WHAT THE SETTINGS MEAN
----------------------
  RestartCount 2 / RestartInterval 10m   two retries, ten minutes apart. Three
                                         attempts still land inside the window.

     Retries cover a NON-ZERO EXIT — the probe could not sample at all. They
     deliberately do NOT cover a run that recorded a per-asset error and exited
     0: re-running that would re-sample the assets that already succeeded, and
     the duplicates would carry extra weight in the percentiles. A partial day
     is recorded as partial; the per-asset coverage contract already refuses to
     call it complete.

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

WHEN A DAY IS LOST
------------------
The task registers with LogonType InteractiveToken — it runs as the user, with
no stored password. That is the right trade (a scheduled measurement is not
worth a password on disk), but it means a day is lost unless:

  * the machine is awake at 00:05 UTC — WakeToRun is off deliberately, and a
    missed day is not made up out of window; and
  * the user is still logged in. A LOCKED screen is fine; signing out is not.

Expect the calendar to run longer than the fourteen days of coverage require.
That is honest sampling, not a fault to work around.
#>

$ErrorActionPreference = "Stop"

# The UTC instant to sample at. Minute-of-day must stay inside the probe's
# execution window; tests assert that against EXECUTION_WINDOW_MINUTES.
$TargetUtcHour = 0
$TargetUtcMinute = 5

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "scripts\run_stf_cost_probe.bat"
if (-not (Test-Path $runner)) { throw "runner not found at $runner" }

# Next occurrence of the target instant, computed in UTC. Never `Get-Date`
# without a kind: the whole defect above came from a local wall-clock reading.
$nowUtc = [DateTime]::UtcNow
$startUtc = [DateTime]::new($nowUtc.Year, $nowUtc.Month, $nowUtc.Day,
                            $TargetUtcHour, $TargetUtcMinute, 0,
                            [DateTimeKind]::Utc)
if ($startUtc -le $nowUtc) { $startUtc = $startUtc.AddDays(1) }

$action = New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root
# -At takes a DateTime; handing it a UTC-kind value keeps the instant right.
# Never a "HH:mm" string -- that is a local wall-clock reading, and it is what
# made the schedule season-dependent.
$trigger = New-ScheduledTaskTrigger -Daily -At $startUtc.ToLocalTime()

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
$settings.StartWhenAvailable = $false

$description = @"
Phase 7R-2: one read-only STF execution-cost observation per day, anchored to
00:05 UTC (displays as 20:05 Eastern daylight time, 19:05 Eastern standard
time) -- five minutes into the protocol's execution window, year round.

Sweeps the PUBLIC order book and places no orders. A missed day is NOT made up
outside the window. Runs as the logged-in user with no stored password: the
machine must be awake and the user signed in, though a locked screen is fine.
"@

Register-ScheduledTask -TaskName "CryptoOrchestra-STF-CostProbe" `
    -Action $action -Trigger $trigger -Settings $settings `
    -Description $description -Force | Out-Null

# Verify the INSTANT, not the spelling: Task Scheduler always restates the
# boundary with the local offset, and an offset-bearing boundary is absolute.
$xml = Export-ScheduledTask -TaskName "CryptoOrchestra-STF-CostProbe"
$boundary = ([xml]$xml).Task.Triggers.CalendarTrigger.StartBoundary
$asUtc = [DateTimeOffset]::Parse($boundary).ToUniversalTime()
Write-Output "StartBoundary: $boundary  =  $($asUtc.ToString('yyyy-MM-ddTHH:mm:ssZ'))"
if ($asUtc.Hour -ne $TargetUtcHour -or $asUtc.Minute -ne $TargetUtcMinute) {
    throw ("StartBoundary is {0:00}:{1:00} UTC, not the required {2:00}:{3:00}" -f `
           $asUtc.Hour, $asUtc.Minute, $TargetUtcHour, $TargetUtcMinute)
}
$minuteOfDay = $asUtc.Hour * 60 + $asUtc.Minute
Write-Output "minute of UTC day: $minuteOfDay (execution window is 90 minutes wide)"
Get-ScheduledTaskInfo -TaskName "CryptoOrchestra-STF-CostProbe" |
    Select-Object TaskName, NextRunTime | Format-List

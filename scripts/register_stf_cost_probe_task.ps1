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
  WakeToRun = $true                      Windows arms an RTC wake timer for the
                                         trigger, takes the machine out of
                                         standby, runs the probe, and lets
                                         normal power management put it back.
                                         This does NOT keep the machine awake.
  DontStopOnIdleEnd                      StopOnIdleEnd is inert while
                                         RunOnlyIfIdle is false, but a
                                         wake-to-run start happens precisely
                                         when the machine is idle. Nothing may
                                         terminate the probe because idle ended.

WAKE TIMERS ARE A POWER-PLAN PRIVILEGE, NOT A TASK SETTING
----------------------------------------------------------
WakeToRun only arms a timer if the ACTIVE power plan permits wake timers. On
this machine both AC and DC were "Disable" (index 0), so the flag would have
been silently inert.

The check at the bottom reads the plan back and reports BOTH rails, warning on
each one that is not enabled. It warns rather than throws, and the distinction
is deliberate: the power plan is machine state, not something this script owns
or should silently rewrite, and the task registration itself has already
succeeded by then. A throw would leave a correctly registered task behind a
non-zero exit. So this script does not promise a hardened schedule — it
promises to say plainly when the machine cannot deliver one. To set it by hand:

    powercfg /setacvalueindex SCHEME_CURRENT SUB_SLEEP RTCWAKE 1
    powercfg /setdcvalueindex SCHEME_CURRENT SUB_SLEEP RTCWAKE 1
    powercfg /setactive SCHEME_CURRENT

"Important Wake Timers Only" (index 2) is NOT enough: a user task's timer is
not an important timer. Index 0 restores the Windows default.

WHEN A DAY IS LOST
------------------
The task registers with LogonType InteractiveToken — it runs as the user, with
no stored password. That is the right trade (a scheduled measurement is not
worth a password on disk), and it is also what keeps the probe in the session
where the OneDrive sync client is running: every path this task touches, the
interpreter included, is a Files-On-Demand reparse point. An S4U/session-0
principal would need no password either, but it would read those placeholders
with no sync engine to hydrate them. Not worth the trade for a daily HTTP read.

With WakeToRun armed a day is still lost if:

  * the machine HIBERNATED rather than slept. An RTC timer cannot resume S4.
    On battery, modern standby hibernates once the standby budget is exhausted,
    so an unattended run is only dependable on AC; and
  * the user has SIGNED OUT. A locked screen is fine; signing out is not.

A missed day is still not made up out of window, and that is deliberate.

Expect the calendar to run longer than the fourteen days of coverage require.
That is honest sampling, not a fault to work around.

OBSERVED, 2026-08-27: StartWhenAvailable IS NOT A GUARANTEE
-----------------------------------------------------------
The first scheduled run did not happen at the trigger. The machine was not
awake at 00:05 UTC, and Windows launched the elapsed daily trigger on resume
instead -- LastRunTime 00:01 local, 04:01 UTC, three hours and fifty-six
minutes late -- with StartWhenAvailable unset, which means false.

Nothing here can fix that, and this script does not pretend to. What held is
the probe itself: it recomputed the window, refused, wrote the reason and
exited 2, so no out-of-hour reading entered the sample. The scheduler is
best-effort; the instrument is the guard. That is why the window check lives in
the probe and not in this file, and why --force is absent from the runner.

A refused late run also trips RestartOnFailure, so expect up to three refusals
in the operational log for one missed day. They record the miss; they cannot
record data.
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
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -DontStopOnIdleEnd
# Still false, and deliberately so: waking for the trigger is reliability, but
# running an ELAPSED trigger late would sample the wrong hour. The probe would
# refuse such a run anyway; not starting it is cheaper and clearer.
$settings.StartWhenAvailable = $false

$description = @"
Phase 7R-2: one read-only STF execution-cost observation per day, anchored to
00:05 UTC (displays as 20:05 Eastern daylight time, 19:05 Eastern standard
time) -- five minutes into the protocol's execution window, year round.

Sweeps the PUBLIC order book and places no orders. A missed day is NOT made up
outside the window. Runs as the logged-in user with no stored password.

WakeToRun is on: Windows wakes the machine from standby for the trigger and
lets it go back to sleep afterwards. A day is still lost if the machine
hibernated (an RTC timer cannot resume S4 -- on battery, modern standby
hibernates once the standby budget runs out, so AC is recommended) or if the
user signed out. A locked screen is fine.
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

# The last retry must still land inside the window, or the retry policy is
# quietly manufacturing runs the probe will refuse.
$lastAttempt = $minuteOfDay + 2 * 10
if ($lastAttempt -gt 90) {
    throw ("last retry lands $lastAttempt minutes into the UTC day, " +
           "outside the 90-minute execution window")
}
Write-Output "attempts at +0/+10/+20 min => last at minute $lastAttempt of the UTC day"

# Reliability settings, read back from the registered task rather than assumed.
$registered = Get-ScheduledTask -TaskName "CryptoOrchestra-STF-CostProbe"
if (-not $registered.Settings.WakeToRun) { throw "WakeToRun did not take" }
if ($registered.Settings.StartWhenAvailable) {
    throw "StartWhenAvailable is on: a missed day could be sampled at the wrong hour"
}
Write-Output ("WakeToRun: {0}   StartWhenAvailable: {1}   MultipleInstances: {2}" -f `
    $registered.Settings.WakeToRun, $registered.Settings.StartWhenAvailable,
    $registered.Settings.MultipleInstances)

# WakeToRun is inert unless the ACTIVE power plan allows wake timers. BOTH
# rails matter and are reported: the task is registered -AllowStartIfOnBatteries
# on purpose, so a DC rail left at Disable silently thins the schedule exactly
# as often as the laptop happens to be unplugged at 00:05 UTC. Index 2
# ("Important Wake Timers Only") does not count -- a user task's timer is not
# an important timer, so only index 1 is accepted.
$rtcwake = (powercfg /query SCHEME_CURRENT SUB_SLEEP `
                BD3B718A-0680-4D9D-8AB2-E1D2B4AC806D) -join "`n"
$rails = [ordered]@{
    AC = [regex]::Match($rtcwake, 'Current AC Power Setting Index: 0x(\w+)').Groups[1].Value
    DC = [regex]::Match($rtcwake, 'Current DC Power Setting Index: 0x(\w+)').Groups[1].Value
}
Write-Output ("power plan 'Allow wake timers': AC=0x{0} DC=0x{1}  (1 = Enable)" -f `
    $rails.AC, $rails.DC)
foreach ($rail in $rails.Keys) {
    $value = $rails[$rail]
    if ([string]::IsNullOrEmpty($value)) {
        Write-Warning ("Could not read the '$rail' wake-timer setting; WakeToRun " +
                       "may be inert. Check it by hand before trusting the schedule.")
        continue
    }
    if ([Convert]::ToInt32($value, 16) -ne 1) {
        Write-Warning ("Wake timers are NOT enabled on $rail (0x$value); WakeToRun " +
                       "cannot wake this machine while it is on $rail power. See " +
                       "the header of this script for the powercfg commands.")
    }
}

Get-ScheduledTaskInfo -TaskName "CryptoOrchestra-STF-CostProbe" |
    Select-Object TaskName, NextRunTime | Format-List

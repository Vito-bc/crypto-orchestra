<#
Registers the credential-free CFM funding monitor with Windows Task Scheduler.

Run from an ordinary PowerShell:

    powershell -ExecutionPolicy Bypass -File scripts\register_cfm_funding_monitor_task.ps1

The trigger is hourly at minute :02. Unlike the execution-cost probe, a late
snapshot still describes the funding rate exposed by the public product record,
so StartWhenAvailable is deliberately true. WakeToRun is enabled, both battery
restrictions are disabled, overlapping runs are ignored, and a hung poll is
stopped after five minutes.
#>

$ErrorActionPreference = "Stop"
$TaskName = "CryptoOrchestra-CFM-FundingMonitor"
$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "scripts\run_cfm_funding_monitor.bat"
if (-not (Test-Path $runner)) { throw "runner not found at $runner" }

$now = Get-Date
$start = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day `
                  -Hour $now.Hour -Minute 2 -Second 0
if ($start -le $now) { $start = $start.AddHours(1) }

$action = New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $start `
    -RepetitionInterval (New-TimeSpan -Hours 1)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -DontStopOnIdleEnd
$settings.StartWhenAvailable = $true

$description = @"
Credential-free hourly snapshots of public BTC and ETH CFM funding. Runs at
minute :02, records to logs\cfm_funding.jsonl, and makes no trading decision.
Late starts are valid snapshots and run when available. Wake and battery runs
are enabled; each invocation is limited to five minutes.
"@

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Description $description -Force | Out-Null

$registered = Get-ScheduledTask -TaskName $TaskName
$xml = [xml](Export-ScheduledTask -TaskName $TaskName)
$calendar = $xml.Task.Triggers.TimeTrigger
$boundary = [DateTimeOffset]::Parse($calendar.StartBoundary)
$interval = $calendar.Repetition.Interval
$limit = $xml.Task.Settings.ExecutionTimeLimit

if ($boundary.Minute -ne 2) { throw "trigger minute is not :02" }
if ($interval -ne "PT1H") { throw "repetition interval is $interval, not PT1H" }
if (-not $registered.Settings.WakeToRun) { throw "WakeToRun did not take" }
if (-not $registered.Settings.StartWhenAvailable) {
    throw "StartWhenAvailable did not take"
}
if ($registered.Settings.DisallowStartIfOnBatteries) {
    throw "battery starts are still restricted"
}
if ($registered.Settings.StopIfGoingOnBatteries) {
    throw "task would stop when switching to battery"
}
if ($limit -ne "PT5M") { throw "execution limit is $limit, not PT5M" }

Write-Output ("Task: {0}" -f $TaskName)
Write-Output ("StartBoundary: {0}  Repetition: {1}" -f `
              $calendar.StartBoundary, $interval)
Write-Output ("WakeToRun: {0}  StartWhenAvailable: {1}" -f `
              $registered.Settings.WakeToRun, `
              $registered.Settings.StartWhenAvailable)
Write-Output ("Battery restrictions: start={0} stop={1}  Limit: {2}" -f `
              $registered.Settings.DisallowStartIfOnBatteries, `
              $registered.Settings.StopIfGoingOnBatteries, $limit)
Get-ScheduledTaskInfo -TaskName $TaskName |
    Select-Object TaskName, LastRunTime, LastTaskResult, NextRunTime | Format-List

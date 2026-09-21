<#
Registers the log-only agent shadow from the primary checkout, never a worktree.

Run from an ordinary PowerShell after this change is present in the primary
checkout:

    powershell -ExecutionPolicy Bypass -File scripts\register_agent_shadow_task.ps1

The task polls after each hourly candle, but model calls are event-triggered:
they occur only when the frozen scanner produces a new WIDE candidate.
#>

param([string]$MainCheckout)

$ErrorActionPreference = "Stop"
$TaskName = "CryptoOrchestra-AgentShadow"

if (-not $MainCheckout) {
    $repo = Split-Path -Parent $PSScriptRoot
    $firstWorktree = git -C $repo worktree list --porcelain |
        Where-Object { $_ -like "worktree *" } |
        Select-Object -First 1
    if (-not $firstWorktree) { throw "could not locate the primary checkout" }
    $MainCheckout = $firstWorktree.Substring("worktree ".Length)
}

$root = (Resolve-Path -LiteralPath $MainCheckout).Path
$runner = Join-Path $root "scripts\run_agent_shadow.bat"
if (-not (Test-Path -LiteralPath $runner)) { throw "runner not found at $runner" }

$now = Get-Date
$start = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day `
                  -Hour $now.Hour -Minute 5 -Second 0
if ($start -le $now) { $start = $start.AddHours(1) }

$action = New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $start `
    -RepetitionInterval (New-TimeSpan -Hours 1)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -DontStopOnIdleEnd
$settings.StartWhenAvailable = $true

$description = @"
Log-only event-triggered agent shadow. Polls the latest closed hourly candle at
minute :05 and calls models only for a new WIDE candidate. Writes observations
to logs\agent_shadow.jsonl; it has no order path. Wake and battery runs are
enabled; each invocation is limited to twenty minutes.
"@

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Description $description -Force | Out-Null

$registered = Get-ScheduledTask -TaskName $TaskName
$xml = [xml](Export-ScheduledTask -TaskName $TaskName)
$calendar = $xml.Task.Triggers.TimeTrigger
$boundary = [DateTimeOffset]::Parse($calendar.StartBoundary)
$interval = $calendar.Repetition.Interval
$limit = $xml.Task.Settings.ExecutionTimeLimit

if ($boundary.Minute -ne 5) { throw "trigger minute is not :05" }
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
if ($limit -ne "PT20M") { throw "execution limit is $limit, not PT20M" }

Write-Output ("Task: {0}" -f $TaskName)
Write-Output ("Primary checkout: {0}" -f $root)
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

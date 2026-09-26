<#
Registers the log-only agent shadow from the primary checkout, never a worktree.

Run from an ordinary PowerShell after this change is present in the primary
checkout:

    powershell -ExecutionPolicy Bypass -File scripts\register_agent_shadow_task.ps1

Re-registration is the activation step for agent-shadow-event-v1. A task
registered by the previous script keeps using run_agent_shadow.bat (wide-v1)
until then.
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
$runner = Join-Path $root "scripts\run_agent_shadow_event.bat"
if (-not (Test-Path -LiteralPath $runner)) { throw "runner not found at $runner" }

$now = Get-Date
$start = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day `
                  -Hour $now.Hour -Minute 5 -Second 0
if ($start -le $now) { $start = $start.AddHours(1) }

# Execution limit: 40 minutes, derived from the worst case the DEFAULT
# candidate cap allows (10 candidates in one catch-up run), not a round guess.
#
#   Per candidate the shadow calls 6 agents SEQUENTIALLY, then the
#   orchestrator. Measured on 1,540 live-pipeline runs in logs\scheduler.log
#   (agents there ran concurrently, so each agent's time is bounded by the
#   whole concurrent stage A; orchestrator time by total T minus A):
#       per-candidate bound  T + 5A:  median 78 s, p99 200 s, max 441 s
#   Frame build for all four assets, measured 2026-09-23 on this host:
#       9.3 s after an hour's gap, 1.4 s warm (~10 s more for attachments).
#
#   10 candidates x 200 s (p99)                    = 2000 s
#   frames + attachments, 6x the measured ~20 s    =  120 s
#   total                                          = 2120 s = 35.3 min
#   one candidate at the observed max + 9 at p99:
#       441 + 9 x 200 + 120                        = 2361 s = 39.4 min
#   -> PT40M.
#
# Exceeding it is safe, not silent: a killed run keeps every decision it
# wrote, and the next run resumes from the shadow log. With IgnoreNew, a run
# still going at the next :05 simply absorbs that slot; the following run's
# resume point covers it.
$action = New-ScheduledTaskAction -Execute $runner -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $start `
    -RepetitionInterval (New-TimeSpan -Hours 1)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 40) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -DontStopOnIdleEnd
$settings.StartWhenAvailable = $true

$description = @"
Log-only event-triggered agent shadow. At minute :05 examines every closed
hourly candle since the last one it recorded (72h look-back cap) and calls
models only for the first WIDE candidate of each distinct EMA50 cross
(agent-shadow-event-v1). Writes observations
to logs\agent_shadow.jsonl; it has no order path. Wake and battery runs are
enabled; each invocation is limited to forty minutes.
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
if ($limit -ne "PT40M") { throw "execution limit is $limit, not PT40M" }

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

# Registers the hourly pipeline task.
# Run ONCE as Administrator.
#
# Changes from old setup:
#   - Runs runner.py directly (single shot) instead of scheduler.py (loop)
#   - Trigger: every 60 minutes (Task Scheduler handles repetition)
#   - StartWhenAvailable: if machine was off at trigger time, runs ASAP on next boot
#   - RestartCount: 3 retries on failure, 5 min apart
#   - MultipleInstances: IgnoreNew (no double runs)

$repoRoot  = Split-Path -Parent $MyInvocation.MyCommand.Path
$batFile   = Join-Path $repoRoot "pipeline\run_once.bat"
$taskName  = "CryptoOrchestra-AgentPipeline"

if (-not (Test-Path $batFile)) { Write-Error "Wrapper not found: $batFile"; exit 1 }

$action = New-ScheduledTaskAction `
    -Execute       "cmd.exe" `
    -Argument      "/c `"$batFile`"" `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 1)
$trigger.Repetition.Duration = ""   # empty = indefinite

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit       (New-TimeSpan -Minutes 10) `
    -RestartCount             3 `
    -RestartInterval          (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable `
    -MultipleInstances        IgnoreNew `
    -RunOnlyIfNetworkAvailable

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName  $taskName `
    -Action    $action `
    -Trigger   $trigger `
    -Settings  $settings `
    -RunLevel  Highest `
    -Force | Out-Null

$info = Get-ScheduledTaskInfo -TaskName $taskName
Write-Host "Registered: $taskName"
Write-Host "  State:    $((Get-ScheduledTask -TaskName $taskName).State)"
Write-Host "  Next run: $($info.NextRunTime)"
Write-Host ""
Write-Host "Task will now run every 60 minutes and recover automatically after reboots."
Write-Host "To run immediately: Start-ScheduledTask -TaskName $taskName"

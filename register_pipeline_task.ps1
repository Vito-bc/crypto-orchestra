# Registers the hourly pipeline task.
# Run ONCE as Administrator.
#
# Runs python.exe directly (no cmd/bat wrapper) to avoid PATH issues in Task Scheduler.
# Trigger: every 30 minutes, StartWhenAvailable so missed runs catch up on next boot.

$repoRoot  = Split-Path -Parent $MyInvocation.MyCommand.Path
$python    = Join-Path $repoRoot "venv\Scripts\pythonw.exe"
$taskName  = "CryptoOrchestra-AgentPipeline"

if (-not (Test-Path $python)) { Write-Error "Python not found: $python"; exit 1 }

$action = New-ScheduledTaskAction `
    -Execute          $python `
    -Argument         "pipeline\runner.py" `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 30)
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
Write-Host "Task will now run every 30 minutes and recover automatically after reboots."
Write-Host "To run immediately: Start-ScheduledTask -TaskName $taskName"

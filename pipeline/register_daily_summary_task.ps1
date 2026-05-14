# Registers a daily 9 AM Telegram summary from the new multi-agent system.
# Run ONCE as Administrator.

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python   = Join-Path $repoRoot "venv\Scripts\pythonw.exe"
$script   = Join-Path $repoRoot "pipeline\daily_summary.py"
$taskName = "CryptoOrchestra-DailySummary"

if (-not (Test-Path $python)) { Write-Error "Python not found: $python"; exit 1 }
if (-not (Test-Path $script)) { Write-Error "Script not found: $script"; exit 1 }

$action  = New-ScheduledTaskAction -Execute $python -Argument $script -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -Daily -At "09:00AM"
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
            -RestartCount 1 -RestartInterval (New-TimeSpan -Minutes 2)

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -RunLevel Highest -Force | Out-Null

Write-Host "Registered: $taskName — runs daily at 09:00 AM"
Write-Host "To run now: Start-ScheduledTask -TaskName '$taskName'"

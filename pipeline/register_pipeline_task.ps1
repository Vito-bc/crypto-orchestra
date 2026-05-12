# Registers the Crypto Orchestra agent pipeline as a Windows scheduled task.
# Runs pipeline/runner.py (BTC + ETH) every hour, silently in background.
# Run this script ONCE as Administrator to set up the task.

$repoRoot  = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python    = Join-Path $repoRoot "venv\Scripts\pythonw.exe"   # no popup window
$script    = Join-Path $repoRoot "pipeline\runner.py"
$taskName  = "CryptoOrchestra-AgentPipeline"

if (-not (Test-Path $python)) {
    Write-Error "Python not found at: $python"; exit 1
}
if (-not (Test-Path $script)) {
    Write-Error "Runner not found at: $script"; exit 1
}

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed existing task."
}

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "pipeline\runner.py" `
    -WorkingDirectory $repoRoot

# Trigger: start 1 minute from now, repeat every hour for 10 years
$startTime = (Get-Date).AddMinutes(1)
$trigger   = New-ScheduledTaskTrigger `
    -Once `
    -At $startTime `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action   $action `
    -Trigger  $trigger `
    -RunLevel Highest `
    -Force | Out-Null

$info = Get-ScheduledTaskInfo -TaskName $taskName
Write-Host ""
Write-Host "Task registered: $taskName"
Write-Host "First run at:   $($info.NextRunTime)"
Write-Host "Then every:     1 hour"
Write-Host "Running:        pythonw.exe (silent, no popup)"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  Run now:  Start-ScheduledTask -TaskName '$taskName'"
Write-Host "  Status:   Get-ScheduledTaskInfo -TaskName '$taskName' | Select LastRunTime, LastTaskResult, NextRunTime"
Write-Host "  Remove:   Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"

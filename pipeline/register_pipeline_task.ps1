# Registers the Crypto Orchestra agent pipeline as a Windows scheduled task.
# Runs pipeline/runner.py every hour for ETH-USD.
# Run this script ONCE as Administrator to set up the task.

$repoRoot   = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python     = Join-Path $repoRoot "venv\Scripts\python.exe"
$script     = Join-Path $repoRoot "pipeline\runner.py"
$taskName   = "CryptoOrchestra-AgentPipeline"

if (-not (Test-Path $python)) {
    Write-Error "Python not found at: $python"
    exit 1
}
if (-not (Test-Path $script)) {
    Write-Error "Runner not found at: $script"
    exit 1
}

$action  = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "pipeline\runner.py" `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Hours 1) -Once -At (Get-Date)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest `
    -Force

Write-Host ""
Write-Host "Task registered: $taskName"
Write-Host "Runs every hour using: $python"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  Run now:   Start-ScheduledTask -TaskName '$taskName'"
Write-Host "  Status:    Get-ScheduledTask  -TaskName '$taskName'"
Write-Host "  Remove:    Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"

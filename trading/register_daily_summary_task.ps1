$ErrorActionPreference = "Stop"

$taskName = "CryptoOrchestra-ETH-DailySummary"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$summaryScript = Join-Path $repoRoot "trading\send_daily_telegram_summary.py"
$pythonExe = Join-Path $repoRoot "venv\Scripts\python.exe"

if (-not (Test-Path $summaryScript)) {
    Write-Error "Summary script not found at $summaryScript"
}

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
}

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$summaryScript`""

$trigger = New-ScheduledTaskTrigger -Daily -At "2:00PM"

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Force | Out-Null

Write-Host ""
Write-Host "Daily summary task registered successfully."
Write-Host "Task Name: $taskName"
Write-Host "Script:    $summaryScript"
Write-Host "Time:      2:00 PM"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  schtasks /Run /TN $taskName"
Write-Host "  schtasks /Query /TN $taskName /V /FO LIST"

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

$taskCommand = "`"$pythonExe`" `"$summaryScript`""

schtasks /Create /SC DAILY /ST 18:00 /TN $taskName /TR $taskCommand /F | Out-Host

Write-Host ""
Write-Host "Daily summary task registered successfully."
Write-Host "Task Name: $taskName"
Write-Host "Command:   $taskCommand"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  schtasks /Run /TN $taskName"
Write-Host "  schtasks /Query /TN $taskName /V /FO LIST"

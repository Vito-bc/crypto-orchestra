$ErrorActionPreference = "Stop"

$taskName = "CryptoOrchestra-ETH-PaperTrade"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$runnerScript = Join-Path $repoRoot "trading\run_paper_trade.ps1"
$powershellExe = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"

if (-not (Test-Path $runnerScript)) {
    Write-Error "Runner script not found at $runnerScript"
}

if (-not (Test-Path $powershellExe)) {
    Write-Error "PowerShell executable not found at $powershellExe"
}

$action = New-ScheduledTaskAction `
    -Execute $powershellExe `
    -Argument "-ExecutionPolicy Bypass -File `"$runnerScript`""

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

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
Write-Host "Task registered successfully."
Write-Host "Task Name: $taskName"
Write-Host "Runner:    $runnerScript"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  schtasks /Run /TN $taskName"
Write-Host "  schtasks /Query /TN $taskName /V /FO LIST"

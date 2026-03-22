$ErrorActionPreference = "Stop"

$taskName = "CryptoOrchestra-ETH-DailySummary"

schtasks /Delete /TN $taskName /F | Out-Host

Write-Host ""
Write-Host "Task removed: $taskName"

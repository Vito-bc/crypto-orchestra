$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$pythonExe = Join-Path $repoRoot "venv\Scripts\python.exe"
$paperTradeScript = Join-Path $repoRoot "trading\paper_trade.py"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
}

if (-not (Test-Path $paperTradeScript)) {
    Write-Error "Paper trade script not found at $paperTradeScript"
}

Set-Location $repoRoot
& $pythonExe $paperTradeScript

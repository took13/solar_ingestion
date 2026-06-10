param(
    [int]$LookbackMinutes = 45,
    [int]$LagMinutes = 15,
    [double]$SleepSeconds = 1.0
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

Set-Location $RepoRoot

& $Python -m scripts.run_solaredge_inverter_technical_nearline `
    --lookback-minutes $LookbackMinutes `
    --lag-minutes $LagMinutes `
    --sleep-seconds $SleepSeconds

exit $LASTEXITCODE

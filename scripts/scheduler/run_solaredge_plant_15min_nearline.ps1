param(
    [int]$WindowMinutes = 15,
    [int]$LagMinutes = 30,
    [double]$SleepSeconds = 2.0,
    [string]$Meters = "PRODUCTION,FEEDIN,PURCHASED,SELFCONSUMPTION"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

Set-Location $RepoRoot

& $Python -m scripts.run_solaredge_all_active_ingest `
    --endpoint both `
    --allow-both `
    --window-minutes $WindowMinutes `
    --lag-minutes $LagMinutes `
    --meters $Meters `
    --sleep-seconds $SleepSeconds

exit $LASTEXITCODE

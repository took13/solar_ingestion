param(
    [int]$LookbackMinutes = 45,
    [int]$LagMinutes = 15,
    [int]$WindowMinutes = 45,
    [double]$SleepSeconds = 2.0
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

function Floor-To5Minute([datetime]$dt) {
    $minute = [math]::Floor($dt.Minute / 5) * 5
    return Get-Date -Year $dt.Year -Month $dt.Month -Day $dt.Day -Hour $dt.Hour -Minute $minute -Second 0
}

Set-Location $RepoRoot

$nowLocal = Get-Date
$endLocalDt = Floor-To5Minute($nowLocal.AddMinutes(-1 * $LagMinutes))
$startLocalDt = Floor-To5Minute($nowLocal.AddMinutes(-1 * $LookbackMinutes))
$startLocal = $startLocalDt.ToString("yyyy-MM-dd HH:mm:ss")
$endLocal = $endLocalDt.ToString("yyyy-MM-dd HH:mm:ss")

Write-Host "SolarEdge sensor nearline window: $startLocal -> $endLocal"

& $Python -m scripts.run_solaredge_sensor_5min_ingest `
    --max-plants 5 `
    --start-local $startLocal `
    --end-local $endLocal `
    --window-minutes $WindowMinutes `
    --sleep-seconds $SleepSeconds

exit $LASTEXITCODE

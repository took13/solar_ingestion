param(
    [int]$LookbackMinutes = 45,
    [int]$LagMinutes = 15,
    [double]$SleepSeconds = 0.0,
    [int]$MaxWorkers = 2,
    [int]$RequestTimeoutSec = 15,
    [switch]$ProfileTiming
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

Set-Location $RepoRoot

$RunnerArgs = @(
    "-m", "scripts.run_solaredge_inverter_technical_nearline",
    "--lookback-minutes", $LookbackMinutes,
    "--lag-minutes", $LagMinutes,
    "--sleep-seconds", $SleepSeconds,
    "--max-workers", $MaxWorkers,
    "--request-timeout-sec", $RequestTimeoutSec
)

if ($ProfileTiming) {
    $RunnerArgs += "--profile-timing"
}

& $Python @RunnerArgs

exit $LASTEXITCODE

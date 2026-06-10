<#
Example Windows Task Scheduler registration for SolarEdge lane.
Run from an elevated PowerShell only after all M5-M8 jobs are committed and smoke-tested.

The tasks are intentionally staggered so API calls do not start at exactly the same minute:
- Plant 15-min power/energy: minute 01, 16, 31, 46
- Sensor/irradiance 5-min mart: minute 04, 19, 34, 49
- Inverter technical nearline: minute 08, 23, 38, 53

Adjust $RepoRoot if the repository is not located at C:\SOLAR\solar_ingestion.
#>

$RepoRoot = "C:\SOLAR\solar_ingestion"
$TaskFolder = "SOLAR_Project"

function Register-SolarToPITask {
    param(
        [string]$TaskName,
        [string]$ScriptPath,
        [string]$StartTime
    )

    $Action = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

    schtasks /Create `
        /TN "$TaskFolder\$TaskName" `
        /TR "$Action" `
        /SC MINUTE `
        /MO 15 `
        /ST $StartTime `
        /F
}

Register-SolarToPITask `
    -TaskName "SOLAREDGE_PLANT_15MIN_NEARLINE" `
    -ScriptPath "$RepoRoot\scripts\scheduler\run_solaredge_plant_15min_nearline.ps1" `
    -StartTime "00:01"

Register-SolarToPITask `
    -TaskName "SOLAREDGE_SENSOR_5MIN_NEARLINE" `
    -ScriptPath "$RepoRoot\scripts\scheduler\run_solaredge_sensor_5min_nearline.ps1" `
    -StartTime "00:04"

Register-SolarToPITask `
    -TaskName "SOLAREDGE_INVERTER_TECHNICAL_NEARLINE" `
    -ScriptPath "$RepoRoot\scripts\scheduler\run_solaredge_inverter_technical_nearline.ps1" `
    -StartTime "00:08"

Write-Host "Registered SolarEdge scheduled tasks. Verify with: schtasks /Query /TN SolarToPI\*"

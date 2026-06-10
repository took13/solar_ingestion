# M9 — SolarEdge Closeout / Scheduler / Monitoring Runbook

## Objective

Close the SolarEdge lane after M5–M8 by defining the operational scheduler, monitoring checks, and GO/NO-GO rules.

SolarEdge is treated as a first-class source equivalent to FusionSolar. Do not close the lane unless plant, inverter, and sensor/irradiance paths are all monitored.

## Current PASS baseline

| Area | Status | Notes |
|---|---:|---|
| Plant 15-min power/energy | PASS | `sitePower` and `energyDetails` nearline/backfill path is available. |
| Inverter inventory | PASS | Active inverter inventory is available from SolarEdge inventory. |
| Inverter technical mart | PASS | `mart.fact_solaredge_inverter_technical_5min` populated from `inverterTechnicalData`. |
| Sensor / irradiance discovery | PASS | SolarEdge has irradiance/weather telemetry. `sensorList` may return 403; inventory fallback is valid. |
| Sensor / irradiance mart | PASS | `mart.fact_solaredge_sensor_5min` populated from `sensorData`. |
| Inverter nearline runner | PASS | M8 hotfix freezes one nearline window per run and classifies no-telemetry responses. |

## Scheduler design

Stagger jobs to avoid starting all SolarEdge calls at the same minute.

| Job | Frequency | Suggested start minute | Script | Notes |
|---|---:|---:|---|---|
| Plant 15-min nearline | Every 15 min | 01 | `scripts/scheduler/run_solaredge_plant_15min_nearline.ps1` | Runs `sitePower` and `energyDetails`. |
| Sensor 5-min nearline | Every 15 min | 04 | `scripts/scheduler/run_solaredge_sensor_5min_nearline.ps1` | Uses local dynamic window: now-45 to now-15 minutes by default. |
| Inverter technical nearline | Every 15 min | 08 | `scripts/scheduler/run_solaredge_inverter_technical_nearline.ps1` | Uses M8 runner: now-45 to now-15 minutes by default. |

Recommended first production cadence:

```text
Plant:    every 15 minutes, window=15 min, lag=30 min
Sensor:   every 15 minutes, lookback=45 min, lag=15 min
Inverter: every 15 minutes, lookback=45 min, lag=15 min
```

## Manual smoke commands before Task Scheduler

Run these from the repository root:

```cmd
cd /d C:\SOLAR\solar_ingestion
.venv\Scripts\activate.bat
```

### 1) Plant 15-min nearline

```cmd
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\scheduler\run_solaredge_plant_15min_nearline.ps1
```

### 2) Sensor 5-min nearline

```cmd
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\scheduler\run_solaredge_sensor_5min_nearline.ps1
```

### 3) Inverter technical nearline

```cmd
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\scheduler\run_solaredge_inverter_technical_nearline.ps1
```

## Register Task Scheduler jobs

Review and edit this example first:

```powershell
scripts\scheduler\register_solaredge_scheduled_tasks.example.ps1
```

Then run from elevated PowerShell:

```powershell
cd C:\SOLAR\solar_ingestion
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\scheduler\register_solaredge_scheduled_tasks.example.ps1
```

Verify:

```cmd
schtasks /Query /TN SolarToPI\* /FO LIST /V
```

## Monitoring SQL

Run:

```sql
:r C:\SOLAR\solar_ingestion\sql\diagnostics\20260610_solaredge_lane_closeout_monitoring.sql
```

If SSMS does not support `:r`, open the file and execute its contents.

Key sections:

1. Raw API health by endpoint.
2. Latest raw failures.
3. Plant power mart freshness.
4. Plant energy mart freshness.
5. Sensor / irradiance mart freshness and coverage.
6. Inverter expected active count vs mart count.
7. Possible `NO_TELEMETRY` inverter responses.
8. Inverter canonical duplicate check.
9. Sensor mart duplicate check.

## GO / NO-GO rules

### GO

SolarEdge lane can be considered operational when all are true:

```text
1. sitePower raw success and mart freshness are OK.
2. energyDetails raw success and mart freshness are OK.
3. sensorData raw success and sensor mart freshness are OK.
4. inverterTechnicalData raw success and inverter mart freshness are OK.
5. Duplicate checks are empty.
6. GC5 or any plant with HTTP 200 tiny payload is classified as NO_TELEMETRY, not pipeline failure.
7. Task Scheduler jobs run successfully for at least 2 consecutive cycles.
```

### NO-GO / Investigate

Investigate before declaring closeout if any are true:

```text
1. Raw endpoint failure count > 0 for current cycle.
2. Mart freshness > 90 minutes during daytime.
3. Duplicate check returns rows.
4. expected_minus_mart_count is greater than possible_no_telemetry_inverters.
5. sensor/irradiance mart has no irradiance group for a plant that previously had irradiance.
6. Task Scheduler runtime overlaps the next cycle repeatedly.
```

## Important operating notes

### Inverter no-telemetry is not automatically a failure

SolarEdge can return HTTP 200 with a very small response body, such as 41 bytes, for an inverter that has no telemetry in the requested nearline window. The M8 hotfix exposes this as possible `NO_TELEMETRY`.

### Sensor is faster than inverter by design

`sensorData` is site-level / gateway-level. `inverterTechnicalData` is per inverter serial. Therefore sensor/irradiance nearline should be much faster than inverter nearline.

### Do not run all jobs at the exact same minute

Plant, sensor, and inverter jobs should be staggered to avoid unnecessary API bursts.

### Do not hardcode API keys

SolarEdge API keys remain per-plant secrets. Do not put keys in scripts, scheduled task arguments, source code, or Git.

## Commit command

After smoke test and monitoring SQL pass:

```cmd
git status

git add README_M9_SOLAREDGE_CLOSEOUT_RUNBOOK.md
git add scripts\scheduler\run_solaredge_plant_15min_nearline.ps1
git add scripts\scheduler\run_solaredge_sensor_5min_nearline.ps1
git add scripts\scheduler\run_solaredge_inverter_technical_nearline.ps1
git add scripts\scheduler\register_solaredge_scheduled_tasks.example.ps1
git add sql\diagnostics\20260610_solaredge_lane_closeout_monitoring.sql

git commit -m "add SolarEdge lane scheduler and monitoring runbook"
```

# solar_ingestion_v2

A production-oriented Huawei FusionSolar / SmartPVMS Northbound API ingestion framework designed for:

- multi-account
- multi-plant
- multi-device-type
- raw-first archival
- generic long-form normalization
- typed curated fact tables
- checkpoint-safe incremental ingestion

## Why v2?

The old pipeline was mainly inverter-centric.
This v2 framework is redesigned as a generic ingestion platform for device telemetry.

It supports:
- devType 1 = inverter
- devType 10 = EMI / environmental sensor
- devType 17 = meter
- devType 63 = logger

## Core principles

1. Raw-first
2. Generic normalization for any metric
3. Typed normalization for known device families
4. Config-driven orchestration
5. Safe incremental loads with overlap
6. SQL Server friendly design

## How to run

1. Install packages
2. Execute SQL scripts in `/sql`
3. Configure `/config/app.yaml`
4. Configure `/config/jobs/dev_history_default.yaml`
5. Run:

```bash
python scripts/run_job.py --job dev_history_default
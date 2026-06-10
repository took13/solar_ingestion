# M8 SolarEdge Inverter Technical Nearline

Purpose: add a controlled nearline runner for SolarEdge inverter-level 5-minute technical telemetry, comparable to the FusionSolar inverter near-real-time lane.

## Files

- `scripts/run_solaredge_inverter_technical_nearline.py`
- `sql/diagnostics/20260610_solaredge_inverter_nearline_check.sql`

## Design

- Endpoint: `inverterTechnicalData`
- Target: all active SolarEdge inverters from `dbo.vw_solaredge_active_inverter`
- Default dynamic window: `now - 45 minutes` to `now - 15 minutes`
- Bucket rule: floor source local timestamp to 5-minute bucket
- Output mart: `mart.fact_solaredge_inverter_technical_5min`
- No API keys are logged or stored.

## Recommended scheduler cadence

Run every 15 minutes, but fetch a delayed window so SolarEdge telemetry has time to settle.

Recommended production command after controlled tests pass:

```cmd
python -m scripts.run_solaredge_inverter_technical_nearline ^
  --lookback-minutes 45 ^
  --lag-minutes 15 ^
  --sleep-seconds 1
```

For controlled rollout, start with `--plant-code`, `--max-inverters`, and/or `--dry-run`.

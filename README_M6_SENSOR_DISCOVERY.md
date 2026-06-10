# SolarEdge M6 Sensor / Irradiance Discovery Patch

Purpose:
- Discover whether the active SolarEdge sites have sensor equipment.
- Specifically verify irradiance sensors before building an irradiance mart.
- Probe sensorData response shape as raw only.

Files:
- scripts/run_solaredge_sensor_discovery_probe.py
- sql/migrations/20260610_solaredge_sensor_inventory_view.sql
- sql/diagnostics/20260610_solaredge_sensor_discovery_check.sql

This milestone intentionally does not create sensor mart tables yet. Build mart only after real sensorData output is confirmed.

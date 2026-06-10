# M7 SolarEdge Sensor / Irradiance 5-minute Mart

Adds controlled SolarEdge sensor telemetry ingest:

- API: `/site/{siteId}/sensors`
- Raw: `raw.api_call_v2` endpoint `sensorData`
- Canonical: `norm.canonical_metric_selected`, device_scope `SENSOR`
- Mart: `mart.fact_solaredge_sensor_5min`

Design notes:

- Trust `sensorData` measurement keys as the source of truth. M6 showed some plant inventory can understate irradiance availability.
- Bucket SolarEdge sensor timestamps to 5-minute boundary, same as inverter technical lane.
- Keep `windSpeed` as `wind_speed_raw` until unit is confirmed from site instrumentation or SolarEdge contract docs.
- `irradiance_wm2_best_effort` uses this preference order: POA, GHI, direct, diffuse.

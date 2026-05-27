-- account 5 must not be enabled
SELECT
    j.job_name,
    t.target_id,
    t.account_id,
    t.plant_code,
    t.dev_type_id,
    t.endpoint_name,
    t.is_enabled
FROM ctl.ingest_target t
JOIN ctl.ingest_job j
    ON j.job_id = t.job_id
WHERE t.account_id = 5
  AND t.is_enabled = 1;

-- history must not use account 4/5
SELECT
    j.job_name,
    t.target_id,
    t.account_id,
    t.plant_code,
    t.dev_type_id,
    t.rotation_enabled,
    t.requested_batch_size,
    t.max_batches_per_run
FROM ctl.ingest_target t
JOIN ctl.ingest_job j
    ON j.job_id = t.job_id
WHERE j.job_name = 'inverter_history_nearline'
  AND t.is_enabled = 1
  AND (
        t.account_id IN (4,5)
     OR t.rotation_enabled <> 0
     OR ISNULL(t.requested_batch_size, t.batch_size) > 10
  );
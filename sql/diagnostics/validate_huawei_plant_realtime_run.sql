SELECT TOP (20)
    j.job_name,
    c.account_id,
    c.plant_code,
    c.dev_type_id,
    c.last_success_end_utc,
    c.last_attempt_end_utc,
    c.last_status,
    c.consecutive_failures,
    c.cooldown_until_utc,
    c.last_error_code,
    c.last_error_message
FROM ctl.ingest_checkpoint c
JOIN ctl.ingest_job j
    ON j.job_id = c.job_id
WHERE j.job_name = 'plant_realtime_online'
ORDER BY c.updated_at_utc DESC;
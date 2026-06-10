/*
SolarEdge Inverter Technical Controlled Backfill Checkpoint

Purpose:
- Track per inverter / endpoint backfill progress.
- Keep this lane separate from plant-level SolarEdge sitePower/energyDetails.
- Do not affect Huawei, Enserve, or PI export lanes.
*/

USE SolarDataDB;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'ctl'
      AND t.name = 'solaredge_inverter_backfill_checkpoint'
)
BEGIN
    CREATE TABLE ctl.solaredge_inverter_backfill_checkpoint
    (
        checkpoint_id bigint IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_solaredge_inverter_backfill_checkpoint PRIMARY KEY,

        source_system_code varchar(50) NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_source DEFAULT ('SOLAREDGE'),
        internal_plant_code nvarchar(100) NOT NULL,
        source_plant_code nvarchar(100) NOT NULL,
        source_device_id nvarchar(100) NOT NULL,
        source_device_name nvarchar(255) NULL,
        endpoint_name varchar(100) NOT NULL,

        requested_start_local datetime2(0) NULL,
        requested_end_local datetime2(0) NULL,
        last_success_start_local datetime2(0) NULL,
        last_success_end_local datetime2(0) NULL,
        last_success_start_utc datetime2(0) NULL,
        last_success_end_utc datetime2(0) NULL,

        last_raw_id bigint NULL,
        last_status varchar(50) NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_last_status DEFAULT ('NEW'),
        consecutive_failures int NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_failures DEFAULT (0),
        total_success_windows int NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_success_windows DEFAULT (0),
        total_failed_windows int NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_failed_windows DEFAULT (0),
        last_error_message nvarchar(1000) NULL,

        inserted_at_utc datetime2(3) NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_inserted DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(3) NOT NULL
            CONSTRAINT DF_solaredge_inv_bf_updated DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT UQ_solaredge_inv_bf_checkpoint UNIQUE
        (
            source_system_code,
            internal_plant_code,
            source_plant_code,
            source_device_id,
            endpoint_name
        )
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_solaredge_inv_bf_checkpoint_status'
      AND object_id = OBJECT_ID('ctl.solaredge_inverter_backfill_checkpoint')
)
BEGIN
    CREATE INDEX IX_solaredge_inv_bf_checkpoint_status
    ON ctl.solaredge_inverter_backfill_checkpoint
    (
        endpoint_name,
        last_status,
        last_success_end_utc,
        internal_plant_code,
        source_device_id
    );
END;
GO

CREATE OR ALTER VIEW ctl.vw_solaredge_inverter_backfill_checkpoint_latest
AS
SELECT
    c.source_system_code,
    c.internal_plant_code,
    c.source_plant_code,
    c.source_device_id AS serial_number,
    c.source_device_name AS inverter_name,
    c.endpoint_name,
    c.requested_start_local,
    c.requested_end_local,
    c.last_success_start_local,
    c.last_success_end_local,
    c.last_success_start_utc,
    c.last_success_end_utc,
    c.last_raw_id,
    c.last_status,
    c.consecutive_failures,
    c.total_success_windows,
    c.total_failed_windows,
    c.last_error_message,
    c.updated_at_utc,
    DATEDIFF(minute, c.last_success_end_utc, SYSUTCDATETIME()) AS age_min_from_last_success_end_utc
FROM ctl.solaredge_inverter_backfill_checkpoint c
WHERE c.source_system_code = 'SOLAREDGE';
GO

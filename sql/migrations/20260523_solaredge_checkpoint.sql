USE SolarDataDB;
GO

IF OBJECT_ID('ctl.solaredge_ingest_checkpoint', 'U') IS NULL
BEGIN
    CREATE TABLE ctl.solaredge_ingest_checkpoint
    (
        checkpoint_id bigint IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_solaredge_ingest_checkpoint PRIMARY KEY,

        source_system_code varchar(50) NOT NULL,
        internal_plant_code varchar(100) NOT NULL,
        source_plant_code varchar(100) NOT NULL,
        endpoint_name varchar(100) NOT NULL,

        last_success_start_local datetime2(0) NULL,
        last_success_end_local datetime2(0) NULL,
        last_success_start_utc datetime2(0) NULL,
        last_success_end_utc datetime2(0) NULL,

        last_raw_id bigint NULL,
        last_status varchar(50) NOT NULL
            CONSTRAINT DF_solaredge_checkpoint_status DEFAULT ('PENDING'),

        consecutive_failures int NOT NULL
            CONSTRAINT DF_solaredge_checkpoint_failures DEFAULT (0),

        last_error_message nvarchar(2000) NULL,

        inserted_at_utc datetime2(0) NOT NULL
            CONSTRAINT DF_solaredge_checkpoint_inserted DEFAULT (SYSUTCDATETIME()),

        updated_at_utc datetime2(0) NOT NULL
            CONSTRAINT DF_solaredge_checkpoint_updated DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT UQ_solaredge_ingest_checkpoint
        UNIQUE
        (
            source_system_code,
            source_plant_code,
            endpoint_name
        )
    );
END;
GO

INSERT INTO ctl.solaredge_ingest_checkpoint
(
    source_system_code,
    internal_plant_code,
    source_plant_code,
    endpoint_name,
    last_status
)
SELECT
    m.source_system_code,
    m.internal_plant_code,
    m.source_plant_code,
    e.endpoint_name,
    'PENDING'
FROM dbo.dim_plant_source_map m
CROSS JOIN
(
    SELECT 'sitePower' AS endpoint_name
    UNION ALL
    SELECT 'energyDetails'
) e
WHERE m.source_system_code = 'SOLAREDGE'
  AND m.is_active = 1
  AND NOT EXISTS
  (
      SELECT 1
      FROM ctl.solaredge_ingest_checkpoint c
      WHERE c.source_system_code = m.source_system_code
        AND c.source_plant_code = m.source_plant_code
        AND c.endpoint_name = e.endpoint_name
  );
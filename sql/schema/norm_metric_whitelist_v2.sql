EXEC sp_rename
    'norm.metric_whitelist',
    'metric_whitelist_legacy_20260527';
GO

CREATE TABLE norm.metric_whitelist (
    source_system_code varchar(50) NOT NULL,
    source_api nvarchar(100) NOT NULL,
    dev_type_id int NOT NULL,
    metric_name nvarchar(128) NOT NULL,

    is_enabled bit NOT NULL DEFAULT (1),
    keep_null bit NOT NULL DEFAULT (0),
    keep_raw_text bit NOT NULL DEFAULT (0),

    target_layer varchar(50) NOT NULL DEFAULT ('mart'),
    use_case nvarchar(200) NULL,
    retention_level varchar(50) NOT NULL DEFAULT ('hot'),
    min_keep_days int NULL,

    created_at_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_metric_whitelist
        PRIMARY KEY (source_system_code, source_api, dev_type_id, metric_name)
);
GO
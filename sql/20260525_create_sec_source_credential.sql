USE [SolarDataDB];
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'sec'
)
BEGIN
    EXEC('CREATE SCHEMA sec');
END
GO

IF OBJECT_ID('sec.source_credential', 'U') IS NULL
BEGIN
    CREATE TABLE sec.source_credential (
        credential_id          int IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_source_credential PRIMARY KEY,

        source_system_code     nvarchar(50)  NOT NULL,
        credential_name        nvarchar(150) NOT NULL,
        credential_type        nvarchar(30)  NOT NULL,

        username               nvarchar(200) NULL,
        secret_value           nvarchar(2000) NULL,

        token_value            nvarchar(4000) NULL,
        token_expires_at_utc   datetime2(0) NULL,

        is_active              bit NOT NULL
            CONSTRAINT DF_source_credential_is_active DEFAULT (1),

        last_used_at_utc       datetime2(0) NULL,
        last_rotated_at_utc    datetime2(0) NULL,

        notes                  nvarchar(1000) NULL,

        created_at_utc         datetime2(0) NOT NULL
            CONSTRAINT DF_source_credential_created_at DEFAULT SYSUTCDATETIME(),

        updated_at_utc         datetime2(0) NOT NULL
            CONSTRAINT DF_source_credential_updated_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UX_source_credential UNIQUE
        (
            source_system_code,
            credential_name
        ),

        CONSTRAINT CK_source_credential_type CHECK
        (
            credential_type IN
            (
                'USER_PASSWORD',
                'API_KEY',
                'BEARER_TOKEN',
                'CLIENT_SECRET'
            )
        )
    );
END
GO

IF OBJECT_ID('sec.vw_source_credential_masked', 'V') IS NULL
BEGIN
    EXEC('
    CREATE VIEW sec.vw_source_credential_masked
    AS
    SELECT
        credential_id,
        source_system_code,
        credential_name,
        credential_type,
        username,
        CASE
            WHEN secret_value IS NULL THEN NULL
            ELSE CONCAT(LEFT(secret_value, 4), REPLICATE(''*'', 8), RIGHT(secret_value, 4))
        END AS secret_value_masked,
        CASE
            WHEN token_value IS NULL THEN NULL
            ELSE CONCAT(LEFT(token_value, 4), REPLICATE(''*'', 8), RIGHT(token_value, 4))
        END AS token_value_masked,
        token_expires_at_utc,
        is_active,
        last_used_at_utc,
        last_rotated_at_utc,
        notes,
        created_at_utc,
        updated_at_utc
    FROM sec.source_credential;
    ');
END
GO
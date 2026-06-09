/*
Milestone 3: SolarEdge inverter technical canonical mappings + mart table

Run in SolarDataDB.
This script is idempotent.
*/

SET XACT_ABORT ON;
BEGIN TRAN;

IF OBJECT_ID('mart.fact_solaredge_inverter_technical_5min', 'U') IS NULL
BEGIN
    CREATE TABLE mart.fact_solaredge_inverter_technical_5min
    (
        internal_plant_code nvarchar(100) NOT NULL,
        source_system_code varchar(50) NOT NULL,
        source_plant_code nvarchar(100) NOT NULL,
        source_device_id nvarchar(100) NOT NULL,
        source_device_name nvarchar(255) NULL,
        collect_time_utc datetime2(0) NOT NULL,
        collect_time_local datetime2(0) NULL,

        active_power_kw decimal(18,6) NULL,
        total_energy_kwh decimal(18,6) NULL,
        dc_voltage_v decimal(18,6) NULL,
        temperature_c decimal(18,6) NULL,
        power_limit_pct decimal(18,6) NULL,

        raw_id bigint NOT NULL,
        data_quality_status varchar(50) NOT NULL CONSTRAINT DF_fact_se_invtech_dq DEFAULT ('OK'),
        inserted_at_utc datetime2(3) NOT NULL CONSTRAINT DF_fact_se_invtech_inserted DEFAULT SYSUTCDATETIME(),
        updated_at_utc datetime2(3) NULL,

        CONSTRAINT PK_fact_solaredge_inverter_technical_5min
            PRIMARY KEY CLUSTERED
            (
                internal_plant_code,
                source_system_code,
                source_device_id,
                collect_time_utc
            )
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('mart.fact_solaredge_inverter_technical_5min')
      AND name = 'IX_fact_se_invtech_site_time'
)
BEGIN
    CREATE INDEX IX_fact_se_invtech_site_time
    ON mart.fact_solaredge_inverter_technical_5min
    (
        source_system_code,
        source_plant_code,
        collect_time_utc
    )
    INCLUDE
    (
        internal_plant_code,
        source_device_id,
        active_power_kw,
        total_energy_kwh,
        raw_id,
        data_quality_status
    );
END;

DECLARE @Mappings TABLE
(
    source_system_code varchar(50),
    endpoint_name varchar(100),
    source_device_scope varchar(50),
    source_metric_name nvarchar(200),
    canonical_metric_code varchar(100),
    canonical_unit_code varchar(50),
    multiplier_to_canonical decimal(18,12),
    target_mart bit,
    target_pi bit,
    target_rca bit,
    target_report bit,
    retention_level varchar(50),
    is_enabled bit
);

INSERT INTO @Mappings
(
    source_system_code,
    endpoint_name,
    source_device_scope,
    source_metric_name,
    canonical_metric_code,
    canonical_unit_code,
    multiplier_to_canonical,
    target_mart,
    target_pi,
    target_rca,
    target_report,
    retention_level,
    is_enabled
)
VALUES
('SOLAREDGE','inverterTechnicalData','INVERTER','totalActivePower','inverter_active_power_w','W',1.000000000000,0,0,0,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','totalActivePower','inverter_active_power_kw','kW',0.001000000000,1,1,1,1,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','totalEnergy','inverter_total_energy_wh','Wh',1.000000000000,0,0,0,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','totalEnergy','inverter_total_energy_kwh','kWh',0.001000000000,1,1,1,1,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','dcVoltage','inverter_dc_voltage_v','V',1.000000000000,1,0,1,1,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','temperature','inverter_temperature_c','degC',1.000000000000,1,0,1,1,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','powerLimit','inverter_power_limit_pct','pct',1.000000000000,1,0,1,1,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','groundFaultResistance','inverter_ground_fault_resistance','raw',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','vL1To2','inverter_v_l1_l2_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','vL2To3','inverter_v_l2_l3_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','vL3To1','inverter_v_l3_l1_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.acCurrent','inverter_l1_ac_current_a','A',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.acCurrent','inverter_l2_ac_current_a','A',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.acCurrent','inverter_l3_ac_current_a','A',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.acVoltage','inverter_l1_ac_voltage_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.acVoltage','inverter_l2_ac_voltage_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.acVoltage','inverter_l3_ac_voltage_v','V',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.acFrequency','inverter_l1_ac_frequency_hz','Hz',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.acFrequency','inverter_l2_ac_frequency_hz','Hz',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.acFrequency','inverter_l3_ac_frequency_hz','Hz',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.activePower','inverter_l1_active_power_kw','kW',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.activePower','inverter_l2_active_power_kw','kW',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.activePower','inverter_l3_active_power_kw','kW',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.reactivePower','inverter_l1_reactive_power_kvar','kVAR',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.reactivePower','inverter_l2_reactive_power_kvar','kVAR',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.reactivePower','inverter_l3_reactive_power_kvar','kVAR',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.apparentPower','inverter_l1_apparent_power_kva','kVA',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.apparentPower','inverter_l2_apparent_power_kva','kVA',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.apparentPower','inverter_l3_apparent_power_kva','kVA',0.001000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L1Data.cosPhi','inverter_l1_cosphi','ratio',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L2Data.cosPhi','inverter_l2_cosphi','ratio',1.000000000000,0,0,1,0,'TECHNICAL',1),
('SOLAREDGE','inverterTechnicalData','INVERTER','L3Data.cosPhi','inverter_l3_cosphi','ratio',1.000000000000,0,0,1,0,'TECHNICAL',1);

MERGE norm.metric_mapping AS tgt
USING @Mappings AS src
ON  tgt.source_system_code = src.source_system_code
AND tgt.endpoint_name = src.endpoint_name
AND tgt.source_device_scope = src.source_device_scope
AND tgt.source_metric_name = src.source_metric_name
AND tgt.canonical_metric_code = src.canonical_metric_code
WHEN MATCHED THEN
    UPDATE SET
        tgt.canonical_unit_code = src.canonical_unit_code,
        tgt.multiplier_to_canonical = src.multiplier_to_canonical,
        tgt.target_mart = src.target_mart,
        tgt.target_pi = src.target_pi,
        tgt.target_rca = src.target_rca,
        tgt.target_report = src.target_report,
        tgt.retention_level = src.retention_level,
        tgt.is_enabled = src.is_enabled
WHEN NOT MATCHED THEN
    INSERT
    (
        source_system_code,
        endpoint_name,
        source_device_scope,
        source_metric_name,
        canonical_metric_code,
        canonical_unit_code,
        multiplier_to_canonical,
        target_mart,
        target_pi,
        target_rca,
        target_report,
        retention_level,
        is_enabled
    )
    VALUES
    (
        src.source_system_code,
        src.endpoint_name,
        src.source_device_scope,
        src.source_metric_name,
        src.canonical_metric_code,
        src.canonical_unit_code,
        src.multiplier_to_canonical,
        src.target_mart,
        src.target_pi,
        src.target_rca,
        src.target_report,
        src.retention_level,
        src.is_enabled
    );

COMMIT TRAN;
GO

SELECT
    source_system_code,
    endpoint_name,
    source_device_scope,
    COUNT(*) AS enabled_mapping_count
FROM norm.metric_mapping
WHERE source_system_code = 'SOLAREDGE'
  AND endpoint_name = 'inverterTechnicalData'
  AND is_enabled = 1
GROUP BY source_system_code, endpoint_name, source_device_scope;
GO

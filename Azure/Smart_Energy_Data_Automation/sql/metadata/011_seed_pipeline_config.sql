/* ============================================================
   Smart Energy Demand Analytics (SEDA)
   Seed Data - PIPELINE_CONFIG
   ============================================================ */

INSERT INTO metadata.PIPELINE_CONFIG
(
    source_system_key,
    pipeline_name,
    source_object_name,
    source_schema,
    destination_layer,
    destination_path,
    load_type,
    watermark_column,
    notebook_name,
    schedule_time,
    is_active,
    created_date,
    modified_date,
    bronze_path,
    silver_path,
    gold_path,
    file_format,
    trigger_type
)
VALUES

/* ============================================================
   1. Customer Database
   Incremental load based on modified_date
   ============================================================ */
(
    1,
    'PL_INGEST_CUSTOMER',
    'customer',
    'dbo',
    'Bronze',
    'bronze/customer/',
    'Incremental',
    'modified_date',
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/customer/',
    'silver/customer/',
    'gold/customer/',
    'Delta',
    'Schedule'
),

/* ============================================================
   2. Billing Database
   Incremental load based on modified_date
   ============================================================ */
(
    2,
    'PL_INGEST_BILLING',
    'billing',
    'dbo',
    'Bronze',
    'bronze/billing/',
    'Incremental',
    'modified_date',
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/billing/',
    'silver/billing/',
    'gold/billing/',
    'Delta',
    'Schedule'
),

/* ============================================================
   3. Smart Meter REST API
   Incremental load based on reading_timestamp
   ============================================================ */
(
    3,
    'PL_INGEST_SMART_METER',
    'meter_readings',
    NULL,
    'Bronze',
    'bronze/meter_readings/',
    'Incremental',
    'reading_timestamp',
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/meter_readings/',
    'silver/meter_readings/',
    'gold/meter_readings/',
    'JSON',
    'Schedule'
),

/* ============================================================
   4. Weather API
   Incremental load based on observation_timestamp
   ============================================================ */
(
    4,
    'PL_INGEST_WEATHER',
    'weather',
    NULL,
    'Bronze',
    'bronze/weather/',
    'Incremental',
    'observation_timestamp',
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/weather/',
    'silver/weather/',
    'gold/weather/',
    'JSON',
    'Schedule'
),

/* ============================================================
   5. Tariff CSV
   Full load
   ============================================================ */
(
    5,
    'PL_INGEST_TARIFF',
    'tariff.csv',
    NULL,
    'Bronze',
    'bronze/tariff/',
    'Full',
    NULL,
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/tariff/',
    'silver/tariff/',
    'gold/tariff/',
    'CSV',
    'Schedule'
),

/* ============================================================
   6. Region CSV
   Full load
   ============================================================ */
(
    6,
    'PL_INGEST_REGION',
    'region.csv',
    NULL,
    'Bronze',
    'bronze/region/',
    'Full',
    NULL,
    'Bronze_to_Silver',
    '02:00:00',
    1,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    'bronze/region/',
    'silver/region/',
    'gold/region/',
    'CSV',
    'Schedule'
);

GO
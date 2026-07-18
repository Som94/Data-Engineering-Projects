/* ============================================================
   Smart Energy Demand Analytics (SEDA)
   Seed Data - WATERMARK

   Watermark records are created only for incremental pipelines.
   ============================================================ */

INSERT INTO metadata.WATERMARK
(
    pipeline_key,
    watermark_column,
    last_watermark_value,
    current_watermark_value,
    last_successful_run_id,
    updated_date
)
VALUES
(
    1,                          -- Customer DB
    'modified_date',
    '1900-01-01 00:00:00',
    NULL,
    NULL,
    SYSUTCDATETIME()
),
(
    2,                          -- Billing DB
    'modified_date',
    '1900-01-01 00:00:00',
    NULL,
    NULL,
    SYSUTCDATETIME()
),
(
    3,                          -- Smart Meter REST API
    'reading_timestamp',
    '1900-01-01 00:00:00',
    NULL,
    NULL,
    SYSUTCDATETIME()
),
(
    4,                          -- Weather API
    'observation_timestamp',
    '1900-01-01 00:00:00',
    NULL,
    NULL,
    SYSUTCDATETIME()
);

GO
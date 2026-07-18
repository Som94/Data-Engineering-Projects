/* ============================================================
   Smart Energy Demand Analytics
   Metadata Database - Indexes and Constraints
   ============================================================ */


/* ------------------------------------------------------------
   1. SOURCE_SYSTEM
   Prevent duplicate source system names
   ------------------------------------------------------------ */
CREATE UNIQUE INDEX UX_SOURCE_SYSTEM_NAME
ON metadata.SOURCE_SYSTEM(source_system_name);


/* ------------------------------------------------------------
   2. PIPELINE_CONFIG
   Prevent duplicate pipeline names
   ------------------------------------------------------------ */
CREATE UNIQUE INDEX UX_PIPELINE_CONFIG_NAME
ON metadata.PIPELINE_CONFIG(pipeline_name);


/* Improve lookup by source system */
CREATE INDEX IX_PIPELINE_CONFIG_SOURCE_SYSTEM_KEY
ON metadata.PIPELINE_CONFIG(source_system_key);


/* Improve ADF metadata-driven lookup for active pipelines */
CREATE INDEX IX_PIPELINE_CONFIG_IS_ACTIVE
ON metadata.PIPELINE_CONFIG(is_active);


/* ------------------------------------------------------------
   3. WATERMARK
   Improve watermark lookup by pipeline
   ------------------------------------------------------------ */
CREATE INDEX IX_WATERMARK_PIPELINE_KEY
ON metadata.WATERMARK(pipeline_key);


/* ------------------------------------------------------------
   4. PIPELINE_RUN
   Improve pipeline execution history lookup
   ------------------------------------------------------------ */
CREATE INDEX IX_PIPELINE_RUN_PIPELINE_KEY
ON metadata.PIPELINE_RUN(pipeline_key);


/* Improve lookup using ADF Pipeline Run ID */
CREATE UNIQUE INDEX UX_PIPELINE_RUN_RUN_ID
ON metadata.PIPELINE_RUN(run_id);


/* ------------------------------------------------------------
   5. AUDIT_LOG
   Improve activity-level audit lookup
   ------------------------------------------------------------ */
CREATE INDEX IX_AUDIT_LOG_PIPELINE_RUN_KEY
ON metadata.AUDIT_LOG(pipeline_run_key);


/* ------------------------------------------------------------
   6. FILE_TRACKER
   Improve file lookup
   ------------------------------------------------------------ */
CREATE INDEX IX_FILE_TRACKER_PIPELINE_KEY
ON metadata.FILE_TRACKER(pipeline_key);


CREATE INDEX IX_FILE_TRACKER_PIPELINE_RUN_KEY
ON metadata.FILE_TRACKER(pipeline_run_key);


/* Prevent same physical file from being registered twice
   for the same pipeline */
CREATE UNIQUE INDEX UX_FILE_TRACKER_PIPELINE_FILE
ON metadata.FILE_TRACKER(pipeline_key, file_path);


/* ------------------------------------------------------------
   7. SCHEMA_REGISTRY
   Improve source schema lookup
   ------------------------------------------------------------ */
CREATE INDEX IX_SCHEMA_REGISTRY_SOURCE_SYSTEM_KEY
ON metadata.SCHEMA_REGISTRY(source_system_key);


/* Prevent duplicate column definitions within the same
   source object and schema version */
CREATE UNIQUE INDEX UX_SCHEMA_REGISTRY_OBJECT_COLUMN_VERSION
ON metadata.SCHEMA_REGISTRY
(
    source_system_key,
    source_object_name,
    column_name,
    schema_version
);


/* ------------------------------------------------------------
   8. REJECTED_RECORDS
   Improve rejected-record lookup
   ------------------------------------------------------------ */
CREATE INDEX IX_REJECTED_RECORDS_PIPELINE_RUN_KEY
ON metadata.REJECTED_RECORDS(pipeline_run_key);


CREATE INDEX IX_REJECTED_RECORDS_PIPELINE_KEY
ON metadata.REJECTED_RECORDS(pipeline_key);

GO
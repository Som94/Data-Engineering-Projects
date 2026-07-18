CREATE TABLE metadata.REJECTED_RECORDS
(
    rejected_record_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    pipeline_run_key BIGINT NOT NULL,

    pipeline_key INT NOT NULL,

    source_record_id VARCHAR(200) NULL,

    rejection_reason VARCHAR(1000) NOT NULL,

    rejection_stage VARCHAR(100) NOT NULL,

    rejected_record_data VARCHAR(MAX) NULL,

    rejected_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    reprocessing_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',

    reprocessed_date DATETIME2 NULL,

    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_REJECTED_RECORDS_PIPELINE_RUN
        FOREIGN KEY (pipeline_run_key)
        REFERENCES metadata.PIPELINE_RUN(pipeline_run_key),

    CONSTRAINT FK_REJECTED_RECORDS_PIPELINE_CONFIG
        FOREIGN KEY (pipeline_key)
        REFERENCES metadata.PIPELINE_CONFIG(pipeline_key)
);
GO
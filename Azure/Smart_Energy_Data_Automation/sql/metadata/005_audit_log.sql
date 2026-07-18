CREATE TABLE metadata.AUDIT_LOG
(
    audit_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    pipeline_run_key BIGINT NOT NULL,

    activity_name VARCHAR(200) NOT NULL,

    activity_type VARCHAR(100) NULL,

    activity_status VARCHAR(50) NOT NULL,

    start_time DATETIME2 NULL,

    end_time DATETIME2 NULL,

    records_read BIGINT NULL,

    records_written BIGINT NULL,

    records_rejected BIGINT NULL,

    error_message VARCHAR(MAX) NULL,

    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_AUDIT_LOG_PIPELINE_RUN
        FOREIGN KEY (pipeline_run_key)
        REFERENCES metadata.PIPELINE_RUN(pipeline_run_key)
);
GO
CREATE TABLE metadata.PIPELINE_RUN
(
    pipeline_run_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    pipeline_key INT NOT NULL,

    run_id VARCHAR(100) NOT NULL,

    start_time DATETIME2 NOT NULL DEFAULT GETDATE(),

    end_time DATETIME2 NULL,

    run_status VARCHAR(50) NOT NULL,

    records_read BIGINT NULL,

    records_written BIGINT NULL,

    records_rejected BIGINT NULL,

    error_message VARCHAR(MAX) NULL,

    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_PIPELINE_RUN_PIPELINE_CONFIG
        FOREIGN KEY (pipeline_key)
        REFERENCES metadata.PIPELINE_CONFIG(pipeline_key)
);
GO
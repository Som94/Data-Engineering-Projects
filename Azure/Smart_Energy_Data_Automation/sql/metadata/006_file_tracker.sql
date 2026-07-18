CREATE TABLE metadata.FILE_TRACKER
(
    file_tracker_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    pipeline_key INT NOT NULL,

    file_name VARCHAR(500) NOT NULL,

    file_path VARCHAR(1000) NOT NULL,

    file_size BIGINT NULL,

    file_modified_date DATETIME2 NULL,

    processing_status VARCHAR(50) NOT NULL,

    processed_date DATETIME2 NULL,

    pipeline_run_key BIGINT NULL,

    error_message VARCHAR(MAX) NULL,

    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_FILE_TRACKER_PIPELINE_CONFIG
        FOREIGN KEY (pipeline_key)
        REFERENCES metadata.PIPELINE_CONFIG(pipeline_key),

    CONSTRAINT FK_FILE_TRACKER_PIPELINE_RUN
        FOREIGN KEY (pipeline_run_key)
        REFERENCES metadata.PIPELINE_RUN(pipeline_run_key)
);
GO
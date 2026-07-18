CREATE TABLE metadata.WATERMARK
(
    watermark_key INT IDENTITY(1,1) PRIMARY KEY,

    pipeline_key INT NOT NULL,

    watermark_column VARCHAR(200) NOT NULL,

    last_watermark_value VARCHAR(200) NULL,

    current_watermark_value VARCHAR(200) NULL,

    last_successful_run_id VARCHAR(100) NULL,

    updated_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_WATERMARK_PIPELINE_CONFIG
        FOREIGN KEY (pipeline_key)
        REFERENCES metadata.PIPELINE_CONFIG(pipeline_key)
);
GO
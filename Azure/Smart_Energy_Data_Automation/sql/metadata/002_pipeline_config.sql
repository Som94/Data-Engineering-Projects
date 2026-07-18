CREATE TABLE metadata.PIPELINE_CONFIG
(
    pipeline_key INT IDENTITY(1,1) PRIMARY KEY,
    source_system_key INT NOT NULL,
    pipeline_name VARCHAR(200) NOT NULL,
    source_object_name VARCHAR(200) NOT NULL,
    source_schema VARCHAR(100) NULL,
    destination_layer VARCHAR(50) NOT NULL,
    destination_path VARCHAR(500) NULL,
    load_type VARCHAR(50) NOT NULL,
    watermark_column VARCHAR(200) NULL,
    notebook_name VARCHAR(200) NULL,
    schedule_time TIME NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    modified_date DATETIME2 NULL,
    bronze_path VARCHAR(500) NULL,
    silver_path VARCHAR(500) NULL,
    gold_path VARCHAR(500) NULL,
    file_format VARCHAR(50) NULL,
    trigger_type VARCHAR(50) NULL,

    CONSTRAINT FK_PIPELINE_CONFIG_SOURCE_SYSTEM
        FOREIGN KEY (source_system_key)
        REFERENCES metadata.SOURCE_SYSTEM(source_system_key)
);
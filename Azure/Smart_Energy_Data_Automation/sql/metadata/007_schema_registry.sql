CREATE TABLE metadata.SCHEMA_REGISTRY
(
    schema_registry_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    source_system_key INT NOT NULL,

    source_object_name VARCHAR(200) NOT NULL,

    column_name VARCHAR(200) NOT NULL,

    data_type VARCHAR(100) NOT NULL,

    is_nullable BIT NOT NULL DEFAULT 1,

    ordinal_position INT NOT NULL,

    schema_version INT NOT NULL DEFAULT 1,

    is_active BIT NOT NULL DEFAULT 1,

    effective_from DATETIME2 NOT NULL DEFAULT GETDATE(),

    effective_to DATETIME2 NULL,

    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_SCHEMA_REGISTRY_SOURCE_SYSTEM
        FOREIGN KEY (source_system_key)
        REFERENCES metadata.SOURCE_SYSTEM(source_system_key)
);
GO
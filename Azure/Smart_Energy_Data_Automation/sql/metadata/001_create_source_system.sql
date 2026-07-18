CREATE TABLE metadata.SOURCE_SYSTEM
(
    source_system_key INT IDENTITY(1,1) PRIMARY KEY,

    source_system_name VARCHAR(100) NOT NULL,

    source_type VARCHAR(50) NOT NULL,

    connection_name VARCHAR(100) NOT NULL,

    linked_service_name VARCHAR(100) NOT NULL,

    authentication_type VARCHAR(50) NOT NULL,

    status VARCHAR(20) NOT NULL,

    connector_secret_name VARCHAR(100),

    created_date DATETIME2 DEFAULT GETDATE(),

    modified_date DATETIME2 DEFAULT GETDATE()
);
GO
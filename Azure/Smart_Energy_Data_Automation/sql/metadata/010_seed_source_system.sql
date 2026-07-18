/* ============================================================
   Smart Energy Demand Analytics
   Seed Data - SOURCE_SYSTEM
   ============================================================ */

INSERT INTO metadata.SOURCE_SYSTEM
(
    source_system_name,
    source_type,
    connection_name,
    linked_service_name,
    authentication_type,
    status,
    connector_secret_name,
    created_date,
    modified_date
)
VALUES

/* Customer Database */
(
    'Customer DB',
    'Azure SQL Database',
    'CustomerDatabaseConnection',
    'LS_CustomerDB',
    'Managed Identity',
    'Active',
    NULL,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
),

/* Billing Database */
(
    'Billing DB',
    'Azure SQL Database',
    'BillingDatabaseConnection',
    'LS_BillingDB',
    'Managed Identity',
    'Active',
    NULL,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
),

/* Smart Meter API */
(
    'Smart Meter REST API',
    'REST API',
    'SmartMeterAPIConnection',
    'LS_SmartMeterAPI',
    'API Key',
    'Active',
    'kv-smart-meter-api-key',
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
),

/* Weather API */
(
    'Weather API',
    'REST API',
    'WeatherAPIConnection',
    'LS_WeatherAPI',
    'API Key',
    'Active',
    'kv-weather-api-key',
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
),

/* Tariff File */
(
    'Tariff CSV',
    'CSV',
    'ADLSConnection',
    'LS_ADLS_Gen2',
    'Managed Identity',
    'Active',
    NULL,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
),

/* Region File */
(
    'Region CSV',
    'CSV',
    'ADLSConnection',
    'LS_ADLS_Gen2',
    'Managed Identity',
    'Active',
    NULL,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
);

GO
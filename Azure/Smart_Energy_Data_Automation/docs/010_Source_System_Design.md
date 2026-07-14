# 010 - Source System Design

## Project

**Smart Energy Demand Analytics**

---

# Purpose

The Smart Energy Demand Analytics platform collects data from multiple operational systems. These source systems provide customer information, smart meter readings, billing transactions, weather information, tariff details, and regional master data.

The data from these systems will be ingested into Azure Data Lake Storage Gen2 using Azure Data Factory and processed through the Medallion Architecture (Bronze, Silver, Gold).

---

# Source Systems Overview

| Source System | Technology | Data Type | Frequency | Format |
|--------------|------------|-----------|-----------|---------|
| Customer Management System | Azure SQL Database | Master Data | Daily | SQL Table |
| Billing System | Azure SQL Database | Transaction Data | Daily | SQL Table |
| Smart Meter System | REST API | Meter Readings | Hourly | JSON |
| Weather Service | REST API | Weather Data | Hourly | JSON |
| Tariff Master | CSV | Master Data | Monthly | CSV |
| Region Master | CSV | Master Data | Weekly / On Demand | CSV |

---

# Source System 1

## Customer Management System

### Technology

Azure SQL Database

### Table

customer_master

### Description

Stores customer information and electricity connection details.

### Columns

| Column | Data Type |
|----------|------------|
| customer_id | INT |
| customer_name | VARCHAR(100) |
| customer_type | VARCHAR(30) |
| mobile_number | VARCHAR(20) |
| email | VARCHAR(100) |
| address | VARCHAR(200) |
| city | VARCHAR(50) |
| state | VARCHAR(50) |
| connection_type | VARCHAR(20) |
| tariff_id | INT |
| region_id | INT |
| meter_id | INT |
| created_date | DATETIME |
| updated_date | DATETIME |
| status | VARCHAR(20) |

---

# Source System 2

## Billing System

### Technology

Azure SQL Database

### Table

billing_transactions

### Description

Stores customer billing and payment information.

### Columns

| Column | Data Type |
|----------|------------|
| bill_id | INT |
| customer_id | INT |
| billing_month | DATE |
| energy_consumed | DECIMAL(10,2) |
| amount | DECIMAL(10,2) |
| tax | DECIMAL(10,2) |
| total_amount | DECIMAL(10,2) |
| payment_status | VARCHAR(20) |
| payment_date | DATE |
| created_date | DATETIME |
| updated_date | DATETIME |

---

# Source System 3

## Smart Meter API

### Technology

REST API

### Endpoint

GET /api/v1/meter/readings

### Description

Provides hourly smart meter readings.

### Sample Response

```json
{
  "meter_id": 10001,
  "reading_timestamp": "2026-07-15T02:00:00",
  "voltage": 231.4,
  "current": 10.5,
  "power_factor": 0.98,
  "energy_consumed_kwh": 3.42,
  "frequency": 50.0
}
```

---

# Source System 4

## Weather API

### Technology

REST API

### Endpoint

GET /api/v1/weather

### Description

Provides weather information used for demand analysis.

### Sample Response

```json
{
  "city": "Bhubaneswar",
  "temperature": 35.8,
  "humidity": 81,
  "wind_speed": 14,
  "pressure": 1008,
  "weather_condition": "Sunny"
}
```

---

# Source System 5

## Tariff Master

### Technology

CSV File

### File Name

tariff_master.csv

### Description

Contains electricity tariff information.

### Columns

- tariff_id
- tariff_name
- customer_type
- price_per_unit
- effective_date
- expiry_date

---

# Source System 6

## Region Master

### Technology

CSV File

### File Name

region_master.csv

### Description

Contains geographical hierarchy used for reporting.

### Columns

- region_id
- state
- district
- city
- zone
- substation
- feeder

---

# Source System Data Flow

Customer Management System
        │
Billing System
        │
Smart Meter API
        │
Weather API
        │
Tariff Master
        │
Region Master
        │
        ▼
Azure Data Factory
        ▼
Bronze Layer

---

# Summary

The project integrates multiple heterogeneous data sources, including relational databases, REST APIs, and flat files. Azure Data Factory orchestrates the ingestion process into Azure Data Lake Storage Gen2, where data is processed through Bronze, Silver, and Gold layers before being consumed by the data warehouse and Power BI dashboards.
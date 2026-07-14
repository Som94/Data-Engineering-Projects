# 012 - Entity Relationship (ER) Diagram

# Project

Smart Energy Demand Analytics

---

# Purpose

This document defines the Entity Relationship (ER) Model for the Smart Energy Demand Analytics Data Warehouse.

The Gold Layer follows a Star Schema consisting of one Fact table and seven Dimension tables.

The ER model is optimized for analytical workloads and Power BI reporting.

---

# Data Warehouse Tables

## Fact Table

FACT_ENERGY_CONSUMPTION

## Dimension Tables

DIM_CUSTOMER

DIM_METER

DIM_DATE

DIM_TIME

DIM_REGION

DIM_WEATHER

DIM_TARIFF

---

# Entity Relationship Diagram

                           DIM_DATE
                               |
                               |
DIM_CUSTOMER -------- FACT_ENERGY_CONSUMPTION -------- DIM_METER
                               |
                               |
                          DIM_REGION
                               |
                               |
                           DIM_TIME
                               |
                               |
                         DIM_WEATHER
                               |
                               |
                          DIM_TARIFF

---

# Relationship Summary

| Parent Table | Child Table | Relationship |
|--------------|------------|--------------|
| DIM_CUSTOMER | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_METER | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_DATE | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_TIME | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_REGION | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_WEATHER | FACT_ENERGY_CONSUMPTION | One to Many |
| DIM_TARIFF | FACT_ENERGY_CONSUMPTION | One to Many |

---

# Primary Keys

## FACT_ENERGY_CONSUMPTION

reading_id

---

## DIM_CUSTOMER

customer_key

---

## DIM_METER

meter_key

---

## DIM_DATE

date_key

---

## DIM_TIME

time_key

---

## DIM_REGION

region_key

---

## DIM_WEATHER

weather_key

---

## DIM_TARIFF

tariff_key

---

# Foreign Keys

FACT_ENERGY_CONSUMPTION contains the following Foreign Keys.

customer_key

meter_key

date_key

time_key

region_key

weather_key

tariff_key

---

# Cardinality

DIM_CUSTOMER (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_METER (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_DATE (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_TIME (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_REGION (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_WEATHER (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

-----------------------------

DIM_TARIFF (1)

↓

FACT_ENERGY_CONSUMPTION (Many)

---

# Surrogate Key Strategy

The Data Warehouse uses Integer Surrogate Keys.

Business Keys from source systems are not used for joins inside the warehouse.

Example

Customer_ID = 100345

Customer_Key = 501

Meter_ID = MTR-90021

Meter_Key = 301

This improves performance and supports Slowly Changing Dimensions.

---

# Slowly Changing Dimension Strategy

The following Dimension tables will implement SCD Type 2.

DIM_CUSTOMER

DIM_METER

DIM_TARIFF

History will be preserved using

effective_from

effective_to

is_current

---

# Fact Table Grain

One record represents

One Customer

One Smart Meter

One Timestamp

One Energy Reading

Example

Customer

↓

10025

Meter

↓

MTR00125

Timestamp

↓

15-Jul-2026 10:00

↓

One Record

---

# Data Flow

Customer Database

Billing Database

Smart Meter API

Weather API

Tariff Master

Region Master

↓

Azure Data Factory

↓

Bronze Layer

↓

Silver Layer

↓

Gold Layer

↓

FACT + DIMENSIONS

↓

Power BI

---

# Performance Strategy

The Fact table will be

Partitioned By

date_key

Optimized using

OPTIMIZE

ZORDER

VACUUM

Delta Lake

---

# Estimated Table Size

FACT_ENERGY_CONSUMPTION

50 Million Records / Day

DIM_CUSTOMER

300,000 Records

DIM_METER

500,000 Records

DIM_REGION

500 Records

DIM_DATE

3650 Records

DIM_TIME

1440 Records

DIM_WEATHER

500,000 Records

DIM_TARIFF

100 Records

---

# Conclusion

The Star Schema design provides fast analytical performance, simplifies Power BI reporting, and supports incremental loading, historical tracking, and scalable analytics using Delta Lake.
# 011 - Data Model Design

# Project

Smart Energy Demand Analytics

---

# Purpose

The purpose of this document is to define the data warehouse model for the Smart Energy Demand Analytics platform.

The Gold Layer follows a Star Schema design consisting of one central fact table and multiple dimension tables. This model is optimized for analytical reporting, dashboarding, and business intelligence.

---

# Business Process

The Smart Energy Demand Analytics platform collects electricity consumption data from smart meters and enriches it with customer, weather, billing, tariff, and regional information.

The processed data is used for reporting, business analytics, and executive dashboards.

---

# Data Warehouse Model

The Gold Layer follows a Star Schema.

One Fact Table

Seven Dimension Tables

---

# Fact Table

FACT_ENERGY_CONSUMPTION

---

# Dimension Tables

DIM_CUSTOMER

DIM_METER

DIM_DATE

DIM_TIME

DIM_REGION

DIM_WEATHER

DIM_TARIFF

---

# Grain

One row in the FACT_ENERGY_CONSUMPTION table represents one smart meter reading for one customer at one timestamp.

Example

Customer 1001

Meter 2001

15-Jul-2026

10:00 AM

↓

One Record

---

# FACT TABLE

## FACT_ENERGY_CONSUMPTION

### Purpose

Stores all electricity consumption transactions received from smart meters.

### Primary Key

reading_id

### Foreign Keys

customer_key

meter_key

date_key

time_key

region_key

weather_key

tariff_key

### Measures

energy_consumed_kwh

voltage

current

frequency

power_factor

peak_demand_kw

reactive_power

bill_amount

outage_minutes

reading_timestamp

---

# Dimension Tables

## DIM_CUSTOMER

### Purpose

Stores customer master information.

### Primary Key

customer_key

### Columns

customer_key

customer_id

customer_name

customer_type

mobile_number

email

city

state

connection_type

status

effective_from

effective_to

is_current

---

## DIM_METER

### Purpose

Stores smart meter information.

### Primary Key

meter_key

### Columns

meter_key

meter_id

serial_number

manufacturer

model

installation_date

firmware_version

status

effective_from

effective_to

is_current

---

## DIM_DATE

### Purpose

Stores calendar information.

### Primary Key

date_key

### Columns

date_key

full_date

day

month

month_name

quarter

year

week

weekday

is_weekend

---

## DIM_TIME

### Purpose

Stores time information.

### Primary Key

time_key

### Columns

time_key

hour

minute

second

shift

peak_hour_flag

---

## DIM_REGION

### Purpose

Stores geographical hierarchy.

### Primary Key

region_key

### Columns

region_key

country

state

district

city

zone

substation

feeder

---

## DIM_WEATHER

### Purpose

Stores weather information.

### Primary Key

weather_key

### Columns

weather_key

temperature

humidity

rainfall

wind_speed

pressure

weather_condition

weather_timestamp

---

## DIM_TARIFF

### Purpose

Stores tariff information.

### Primary Key

tariff_key

### Columns

tariff_key

tariff_name

customer_type

price_per_unit

effective_from

effective_to

---

# Relationships

FACT_ENERGY_CONSUMPTION

↓

DIM_CUSTOMER

DIM_METER

DIM_DATE

DIM_TIME

DIM_REGION

DIM_WEATHER

DIM_TARIFF

Every Fact record references exactly one record from each Dimension table.

---

# Surrogate Keys

All Dimension tables use integer surrogate keys.

Business keys from source systems are not used as foreign keys inside the data warehouse.

Example

Customer_ID = 10025

Customer_Key = 501

---

# Slowly Changing Dimensions

SCD Type 2 will be implemented for

DIM_CUSTOMER

DIM_METER

DIM_TARIFF

This allows historical tracking whenever customer, meter, or tariff information changes.

---

# Partition Strategy

FACT_ENERGY_CONSUMPTION

Partition By

date_key

Optional

region_key

---

# Expected Data Volume

FACT_ENERGY_CONSUMPTION

Approximately

50 Million Records Per Day

DIM_CUSTOMER

300,000 Records

DIM_METER

500,000 Records

DIM_REGION

500 Records

DIM_DATE

3,650 Records

DIM_TIME

1,440 Records

DIM_WEATHER

500,000 Records

DIM_TARIFF

100 Records

---

# Data Flow

Source Systems

↓

Azure Data Factory

↓

Bronze Layer

↓

Silver Layer

↓

Gold Layer

↓

Star Schema

↓

Power BI

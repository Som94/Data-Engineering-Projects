# 019 - MySQL Source Dataset

## 1. Objective

Create a reusable and parameterized MySQL dataset in Azure Data Factory that can dynamically read data from different MySQL source tables. This dataset eliminates the need to create separate datasets for each table and supports the metadata-driven ingestion framework.

---

## 2. Business Requirement

The Smart Energy Demand Analytics (SEDA) project ingests data from multiple MySQL source tables, including Customer, Billing, Weather, Tariff, and Region.

Creating a separate dataset for every table would increase maintenance effort and reduce scalability. To follow enterprise best practices, a single parameterized dataset is created. The table name is supplied dynamically at runtime from the metadata configuration.

---

## 3. Dataset Details

| Property | Value |
|----------|-------|
| Dataset Name | DS_MySQL_Source |
| Dataset Type | MySQL |
| Linked Service | LS_MySQL_Source |
| Source Database | energy_source_db |

---

## 4. Dataset Parameter

The dataset contains one parameter.

| Parameter | Type | Description |
|-----------|------|-------------|
| TableName | String | Name of the MySQL table to be read dynamically |

---

## 5. Dynamic Table Configuration

Instead of selecting a fixed table, the dataset uses a dynamic expression.

Dynamic Expression

```text
@dataset().TableName
```

At runtime, Azure Data Factory passes the required table name to this parameter. For example:

- customer
- billing
- weather
- tariff
- region

The same dataset can therefore read data from any configured source table.

---

## 6. Integration with Metadata Framework

The value of the **TableName** parameter is supplied from the **PIPELINE_CONFIG** metadata table.

Example flow:

PIPELINE_CONFIG
        │
        ▼
source_object_name
        │
        ▼
PL_MASTER_INGESTION
        │
        ▼
PL_INGEST_SOURCE
        │
        ▼
DS_MySQL_Source
        │
        ▼
MySQL Source Table

This design allows the pipeline to ingest multiple tables without modifying the dataset.

---

## 7. Validation

Validation steps performed:

- Successfully created DS_MySQL_Source.
- Linked Service configured successfully.
- Dataset parameter TableName created.
- Dynamic expression configured using @dataset().TableName.
- Dataset published successfully.

---

## 8. Screenshots

Add the following screenshots:

- Dataset Overview
- Linked Service Selection
- Parameters Tab
- Dynamic Content Expression
- Connection Configuration

---

## 9. Challenges

- Understanding the difference between a Linked Service and a Dataset.
- Configuring dynamic parameters instead of selecting a fixed source table.
- Mapping metadata values to dataset parameters.

---

## 10. Learnings

A parameterized dataset improves reusability, scalability, and maintainability. Instead of creating one dataset for every source table, a single dataset can read multiple tables by accepting the table name as a parameter. This is a common enterprise design pattern used in metadata-driven Azure Data Factory solutions.

---

## 11. Outcome

Successfully created a reusable MySQL dataset that supports dynamic table selection. This dataset will be used by the metadata-driven ingestion pipeline to load multiple MySQL source tables into the Bronze layer of Azure Data Lake Storage Gen2.
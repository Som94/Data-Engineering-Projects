# 018 - MySQL Linked Service

## 1. Objective

Create a secure connection between Azure Data Factory and the MySQL source database using the Self-hosted Integration Runtime.

---

## 2. Business Requirement

Azure Data Factory requires a Linked Service to communicate with the MySQL source system.

The Self-hosted Integration Runtime securely bridges Azure and the local MySQL instance.

---

## 3. Configuration

Linked Service Name

LS_MySQL_Source

Source

MySQL

Integration Runtime

SHIR_SEDA_001

Server

localhost

Port

3306

Database

energy_source_db

Authentication

Basic

---

## 4. Validation

Test Connection

Expected Result

Connection Successful

---

## 5. Screenshots

(Add screenshots)

---

## 6. Learnings

The Linked Service stores connection information that enables Azure Data Factory activities to access the MySQL source database through the Self-hosted Integration Runtime.
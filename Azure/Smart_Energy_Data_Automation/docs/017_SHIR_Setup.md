# 017 - Self-hosted Integration Runtime (SHIR)

## 1. Objective

Configure a Self-hosted Integration Runtime (SHIR) in Azure Data Factory to securely connect Azure services with the local MySQL database.

---

## 2. Business Requirement

The source database (MySQL) is hosted on a local machine.

Azure Data Factory cannot directly access on-premises or local resources.

A Self-hosted Integration Runtime acts as a secure bridge between Azure Data Factory and the local environment.

---

## 3. Architecture

MySQL
    │
    ▼
Self-hosted Integration Runtime
    │
    ▼
Azure Data Factory
    │
    ▼
Azure Data Lake Storage Gen2

---

## 4. Configuration

Integration Runtime Name

SHIR_SEDA_001

Type

Self-hosted

Machine

Local Windows Machine

Status

Running

---

## 5. Validation

Verify:

Manage
→ Integration Runtime

Status

Running

---

## 6. Screenshots

(Add screenshots here)

---

## 7. Challenges

Example:

- Firewall configuration
- Internet connectivity
- Registration key issues

---

## 8. Learnings

A Self-hosted Integration Runtime provides secure connectivity between Azure Data Factory and local/on-premises data sources without exposing the source database directly to the internet.
# 015 - Low Level Design (LLD)

## Objective

This document describes the detailed execution flow of the Smart Energy Demand Analytics platform.

Unlike the High Level Design, this document focuses on Azure Data Factory activities, Databricks notebook execution, metadata-driven orchestration, incremental loading, audit logging, and error handling.

---

# Execution Flow

1. Daily Trigger starts at 02:00 AM.

2. Read SOURCE_SYSTEM metadata.

3. Read PIPELINE_CONFIG metadata.

4. Execute active pipelines using ForEach.

5. Determine load type.

6. Read WATERMARK.

7. Extract source data.

8. Load Bronze Layer.

9. Execute Bronze → Silver notebook.

10. Execute Silver → Gold notebook.

11. Load Azure Synapse.

12. Update WATERMARK.

13. Insert PIPELINE_RUN.

14. Insert AUDIT_LOG.

15. Send Success Notification.

---

# Failure Flow

If any activity fails:

- Retry Activity
- Log Error
- Insert Audit Record
- Send Failure Notification
- Stop Current Pipeline
- Continue Remaining Pipelines
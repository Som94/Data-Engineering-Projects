# 014 - Metadata & Control Database Design

## Objective

The Metadata Database controls the execution of all Azure Data Factory pipelines.

Instead of hardcoding pipeline logic, ADF dynamically reads metadata from control tables.

This enables:

- Metadata-driven execution
- Incremental loading
- Pipeline monitoring
- Audit logging
- Error handling
- File tracking
- Schema management

---

## Metadata Tables

1. source_system

2. pipeline_config

3. watermark

4. pipeline_run

5. audit_log

6. file_tracker

7. schema_registry

8. rejected_records
# Pipeline Runbook — Execution, Validation, and Troubleshooting

This runbook covers the project's two tested Lakeflow workflows and the standalone large-file benchmark. The scripts use synthetic data only.

## 1. Prerequisites

- Databricks workspace with Unity Catalog catalog `workspace`, schemas `bronze`, `silver`, `gold`.
- Managed Volumes `workspace.bronze.logistic_landing` and `workspace.bronze.pipeline_state`.
- Notebooks available in a Databricks workspace Git folder; table and Volume privileges granted.
- Serverless compute available for notebook tasks and a SQL Warehouse for SQL Editor queries.

**Do not** use one checkpoint for two concurrently running streams or delete it between incremental arrivals.

## 2. Shipment medallion workflow

**Job:** `retail-logistics-medallion-pipeline`  
**Tasks:** `01_ingest_bronze` → `02_transform_silver` → `03_build_gold`

Upload the sample CSV into `/Volumes/workspace/bronze/logistic_landing/shipments.csv`, then trigger the job from **Jobs & Pipelines → Jobs → retail-logistics-medallion-pipeline → Run now**.

Validate:

```sql
SELECT COUNT(*) AS sample_shipments FROM workspace.bronze.shipments;
SELECT COUNT(*) AS silver_shipments FROM workspace.silver.shipments;
SELECT * FROM workspace.gold.delivery_kpis;
```

For the baseline sample, Bronze contains 5 shipments and Gold has 2 rows (one per distribution center).

## 3. JSON incremental ingestion workflow

**Job:** `retail-logistics-json-streaming-pipeline`  
**Tasks:** `ingest_tracking_bronze` → `transform_tracking_silver`  
**Source directory:** `/Volumes/workspace/bronze/logistic_landing/tracking_events/`  
**Checkpoint:** `/Volumes/workspace/bronze/pipeline_state/tracking_events_checkpoint`  
**Target tables:** `workspace.bronze.tracking_events`, `workspace.silver.tracking_events`

### Incremental demonstration

1. Generate `batch_001.json` and `batch_002.json` locally (10,000 JSONL records each).
2. Upload **only** `batch_001.json` to the source directory.
3. Run the JSON Lakeflow Job (or run notebook `06`, then `07`). Verify 10,000 Bronze rows.
4. Upload `batch_002.json` to the **same** directory, using a new filename.
5. Run the job again **without changing the checkpoint**. Verify 20,000 Bronze rows.
6. Run a third time with no new files. The count should remain 20,000 if no other data has been added.

The third-run unchanged-count check is a reproducible validation step. Do not confuse it with event-level deduplication: Auto Loader tracks input files while Silver deduplicates event IDs.

### Validation SQL

```sql
-- Bronze source file distribution
SELECT source_file, COUNT(*) AS events
FROM workspace.bronze.tracking_events
GROUP BY source_file
ORDER BY source_file;

-- Bronze volume and uniqueness
SELECT COUNT(*) AS total_events,
       COUNT(DISTINCT event_id) AS unique_event_ids
FROM workspace.bronze.tracking_events;

-- Silver data quality (observed 2026-11-09: 20000, 20000, 0)
SELECT COUNT(*) AS total_events,
       COUNT(DISTINCT event_id) AS unique_events,
       COUNT_IF(event_timestamp IS NULL) AS invalid_timestamps
FROM workspace.silver.tracking_events;

-- Delivery status distribution
SELECT event_type, COUNT(*) AS events
FROM workspace.silver.tracking_events
GROUP BY event_type
ORDER BY events DESC;
```

### Inspect job status

Navigate to **Jobs & Pipelines → Jobs → retail-logistics-json-streaming-pipeline → Runs**, then select the latest run. The task graph should show Bronze before Silver. Click a task for logs, errors and execution details.

**Confirmed run:** manual execution on November 9, 2026, successful, approximately 1 minute 6 seconds. This is a job-run duration, **not an Auto Loader throughput benchmark**.

## 4. Common problems

| Symptom | Likely cause | Check / action |
| --- | --- | --- |
| `dbutils.fs.ls(SOURCE_PATH)` returns `[]` | File uploaded to a different Volume or folder | Check Catalog Explorer; `batch_001.json` belongs under `logistic_landing/tracking_events/`, not `pipeline_state/` |
| Bronze stays at 10,000 after uploading batch 2 | Ingestion write cell not rerun; new file missing/wrong location | Check `dbutils.fs.ls(SOURCE_PATH)` and run the actual `writeStream...toTable(...)` cell or job |
| Repeated run reads no new rows | No new JSON files discovered | Expected with an intact checkpoint |
| Silver count differs from Bronze | Invalid rows filtered or duplicate event IDs removed | Inspect validation predicates and distinct event IDs |
| Job fails or is queued | Compute, permissions or task errors | Inspect task-level run details and notebook output |
| JSON source mistakenly in checkpoint Volume | Upload used wrong Volume | Move the source file to the landing directory before rerunning ingestion |

## 5. Large-file benchmark

The two standalone notebooks ingest distinct 100k shipment files into `workspace.bronze.shipments_large_csv` and `workspace.bronze.shipments_large_parquet`.

```sql
SELECT 'CSV' AS format, COUNT(*) AS rows
FROM workspace.bronze.shipments_large_csv
UNION ALL
SELECT 'Parquet', COUNT(*)
FROM workspace.bronze.shipments_large_parquet;
```

Baseline measured times: CSV 7.43s, Parquet 4.05s. They are single-run elapsed workflow measurements. See `docs/ingestion_benchmarks.md`.

## 6. Operational safeguards

- Do not reset a streaming checkpoint while preserving an existing Bronze table unless you intentionally plan a replay/deduplication strategy.
- Avoid parallel instances using the same checkpoint.
- Keep generated JSON batches and checkpoint data out of Git.
- Use a new feature branch and pull request for code/documentation updates.
- Monitor task status and downstream row counts; a green job means tasks finished, not that all business data quality expectations have automatically been met.

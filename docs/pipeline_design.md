# Pipeline Design — Manufacturing Quality Pipeline

## Problem Statement

Manufacturing operations generate two concurrent data streams: batch quality inspection records from production machines, and real-time temperature readings from floor sensors. Without a unified pipeline, these arrive as disconnected flat files and raw feeds with no quality enforcement, no audit trail, and no consistent delivery to reporting tools.

This pipeline solves that by applying medallion architecture on Databricks to unify, clean, and aggregate both streams into a single operational metrics table consumed by MySQL.

---

## Layer Design Decisions

### Bronze — Why no transformations?

Bronze is the raw landing zone and the only complete audit trail. If a downstream bug corrupts Silver or Gold, Bronze is the recovery point. Applying transformations here would make root cause analysis harder — you'd lose the ability to distinguish "was this data bad at ingestion or did we introduce a bug?"

In production, Bronze also serves compliance and lineage requirements. Regulators often want to see the original data exactly as it arrived.

### Silver — Why keep valid FAILs?

A valve with quality_score = 61 and result = FAIL is valid data — it tells us something real about the weld process. Silver removes *unprocessable* records (NULLs where a metric is required, sensor spikes that indicate hardware malfunction) but preserves valid failures. Removing them would silently underreport the defect rate in Gold.

The distinction: NULL quality_score means we cannot evaluate this record. Score of 61 means we evaluated it and it failed. These are different problems.

### Gold — Why aggregate at the process_stage level?

Ops teams don't need row-level valve data — they need to know which stage of production is breaking down. Aggregating by process_stage gives them a decision-making unit: "weld has a 50% defect rate, press_fit is clean." That drives resource allocation and investigation prioritization.

The status_flag (OK / MONITOR / ACTION REQUIRED) converts a numeric defect rate into an actionable alert without requiring the consumer to know the threshold logic.

### Delivery — Why overwrite mode to MySQL?

The Gold table represents the current state of quality metrics for the latest production run. Ops teams want the most recent picture, not a historical accumulation in their reporting table. Overwrite ensures they always see fresh data without stale rows from previous runs.

For audit or trend analysis, a separate append-mode table with a `run_id` and `run_date` column would be the correct pattern — that's a natural extension of this pipeline.

---

## Trade-offs Considered

**Auto Loader vs manual batch read**
This pipeline uses direct table reads for simplicity. In production with high-volume file drops, Auto Loader (`cloudFiles` format) would be preferred — it tracks which files have been processed, handles schema evolution, and scales to millions of files without re-reading already-processed data.

**Overwrite vs MERGE in Silver**
Silver currently uses overwrite mode. For production with late-arriving data or corrections, a MERGE (upsert) on `record_id` would be more appropriate — it updates existing records rather than reprocessing the full table. Overwrite was chosen here for clarity in demonstrating the pipeline structure.

**Single-node vs partitioned Gold**
With small data volumes, partitioning adds overhead without benefit. At production scale (millions of valve records), partitioning Gold by `process_stage` and `reported_at` date would dramatically improve query performance for downstream analysts filtering by stage or time window.

---

## Extension Points

- Add **Auto Loader** for file-based Bronze ingestion at scale
- Add **MERGE** logic in Silver for late-arriving and corrected records
- Add **data quality metrics table** tracking row counts and drop rates per run
- Add **Databricks Workflow** with task dependencies and alert on failure
- Add **Unity Catalog** for fine-grained access control on Gold tables
- Add **Structured Streaming** for true real-time sensor processing

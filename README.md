# Manufacturing Quality Pipeline
### End-to-End Medallion Architecture on Databricks

A production-grade data engineering pipeline that ingests hybrid batch and streaming manufacturing data, applies multi-layer quality transformations using the medallion architecture (Bronze → Silver → Gold), and delivers business-ready metrics to a relational database for operational reporting.

---

## Architecture

```
Batch CSVs (machine records)  ─┐
                                ├──► Bronze (raw Delta) ──► Silver (cleaned Delta) ──► Gold (metrics Delta) ──► MySQL / JDBC
Sensor stream (real-time)     ─┘
                                                    ▲
                                         Databricks Workflow
                                    (orchestration + scheduling)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Databricks (Serverless) |
| Storage format | Delta Lake |
| Transformation | PySpark, Spark SQL |
| Orchestration | Databricks Workflows |
| Delivery | JDBC → MySQL |
| Language | Python 3 |

---

## Pipeline Layers

### Bronze — Raw Ingestion
- Lands batch CSV records and simulated sensor stream data as-is into Delta tables
- No transformations — full audit trail preserved
- Adds `ingested_at` metadata timestamp and `source` tag
- Demonstrates: Auto Loader pattern, Delta write, transaction log

### Silver — Cleaning & Validation
- Drops records with NULL quality scores (unprocessable)
- Casts string timestamps to proper `TimestampType`
- Filters sensor spike anomalies (readings > 400°)
- Adds `quality_flag` (HIGH / MEDIUM / LOW) and `temp_flag` (NORMAL / ABNORMAL)
- Demonstrates: PySpark transformations, schema enforcement, data quality rules

### Gold — Business Metrics
- Aggregates by process stage: total valves, pass/fail counts, avg quality score, defect rate %
- Adds `status_flag` (OK / MONITOR / ACTION REQUIRED) for ops alerting
- Demonstrates: Spark aggregations, business logic, reporting-ready output

### Delivery — MySQL via JDBC
- Writes Gold layer to MySQL using JDBC connector
- Includes production-ready connection pattern with credential externalization
- Demonstrates: cross-platform delivery, JDBC sink, overwrite strategy

---

## Key Concepts Demonstrated

- **Medallion architecture** — strict Bronze / Silver / Gold separation with clear promotion logic
- **Delta Lake ACID transactions** — every layer write is atomic; full transaction history via `DESCRIBE HISTORY`
- **Time travel** — all tables support `VERSION AS OF` and `TIMESTAMP AS OF` queries for auditing and recovery
- **Hybrid ingestion** — batch file processing alongside simulated streaming sensor data in one unified pipeline
- **Data quality enforcement** — null handling, outlier filtering, and schema casting at the Silver layer
- **Business metric delivery** — Gold layer surfaces defect rates and quality scores directly consumable by ops teams
- **Workflow orchestration** — Databricks Workflows with task dependencies for end-to-end scheduling and retry logic

---

## Repo Structure

```
manufacturing-quality-pipeline/
│
├── notebooks/
│   └── manufacturing_quality_pipeline.ipynb   # Full pipeline notebook (Databricks-ready)
│
├── scripts/
│   ├── 01_bronze_ingestion.py                 # Bronze layer logic
│   ├── 02_silver_transformation.py            # Silver layer logic
│   ├── 03_gold_aggregation.py                 # Gold layer logic
│   └── 04_delivery_jdbc.py                    # MySQL delivery logic
│
├── data/
│   └── sample/
│       └── sample_batch_records.csv           # Sample input data for testing
│
├── docs/
│   └── pipeline_design.md                     # Design decisions and trade-offs
│
└── README.md
```

---

## Running the Pipeline

### Prerequisites
- Databricks workspace (Community Edition works for all layers except JDBC delivery)
- Python 3.8+
- Delta Lake enabled on cluster

### Steps
1. Clone this repo or import `notebooks/manufacturing_quality_pipeline.ipynb` into Databricks
2. Attach to a cluster with Serverless or Delta Lake support
3. Run cells sequentially (or use **Run All**)
4. For MySQL delivery, update JDBC credentials in `scripts/04_delivery_jdbc.py`

### Workflow Setup
Import the notebook into Databricks Workflows as three sequential tasks:
- Task 1: `01_bronze_ingestion` → Task 2: `02_silver_transformation` → Task 3: `03_gold_aggregation + 04_delivery`

---

## Sample Output

**Gold layer — Quality metrics by process stage:**

| process_stage | total_valves | passed | failed | avg_quality_score | defect_rate_pct | status_flag |
|---|---|---|---|---|---|---|
| weld | 2 | 1 | 1 | 78.25 | 50.0 | ACTION REQUIRED |
| press_fit | 3 | 3 | 0 | 94.77 | 0.0 | OK |
| seal_test | 1 | 1 | 0 | 99.10 | 0.0 | OK |

---

## Design Decisions

**Why Delta Lake over Parquet?**
Delta provides ACID guarantees, time travel for pipeline recovery, and schema enforcement — all critical for a production quality pipeline where late-arriving or corrupted records are common.

**Why medallion architecture?**
Separating raw ingestion (Bronze) from cleaned data (Silver) from business metrics (Gold) allows each layer to serve a different consumer — data engineers debug at Bronze, analysts query Silver, ops teams consume Gold — without any layer contaminating another.

**Why keep failed records in Silver?**
Silver removes *unprocessable* records (nulls, unparseable fields) but preserves *valid failures* like a valve with a low quality score. Removing valid failures would hide real operational problems from the Gold layer.

---

## Author

Daniel Nduka — Data Engineer  
[LinkedIn](https://linkedin.com/in/your-profile) · [GitHub](https://github.com/your-username)  
Databricks Certified Data Engineer Associate (2024)

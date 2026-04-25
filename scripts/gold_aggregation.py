# =============================================================================
# Gold Layer — Business Metrics & Aggregation
# Manufacturing Quality Pipeline
# =============================================================================
# Purpose : Aggregate Silver data into business-ready quality metrics.
#           This is the layer ops teams and analysts consume directly.
# Inputs  : silver_batch (Delta table)
# Outputs : gold_quality_metrics (Delta table)
#
# Metrics produced per process stage:
#   - Total valves processed
#   - Pass / fail counts
#   - Average and minimum quality score
#   - Defect rate percentage
#   - Operational status flag (OK / MONITOR / ACTION REQUIRED)
# =============================================================================

from pyspark.sql import functions as F


def build_gold_metrics(spark):
    """
    Aggregates Silver batch records into process-stage quality metrics.

    Defect rate drives the status_flag which ops teams use for triage:
      - ACTION REQUIRED : defect_rate > 30% — immediate investigation needed
      - MONITOR         : defect_rate > 10% — watch closely
      - OK              : defect_rate <= 10% — within acceptable threshold
    """
    gold_metrics = (
        spark.table("silver_batch")
        .groupBy("process_stage")
        .agg(
            F.count("*").alias("total_valves"),
            F.sum(F.when(F.col("result") == "PASS", 1).otherwise(0)).alias("passed"),
            F.sum(F.when(F.col("result") == "FAIL", 1).otherwise(0)).alias("failed"),
            F.round(F.avg("quality_score"), 2).alias("avg_quality_score"),
            F.min("quality_score").alias("min_quality_score"),
            F.round(
                F.sum(F.when(F.col("result") == "FAIL", 1).otherwise(0)) /
                F.count("*") * 100, 1
            ).alias("defect_rate_pct"),
        )
        .withColumn(
            "status_flag",
            F.when(F.col("defect_rate_pct") > 30, "ACTION REQUIRED")
             .when(F.col("defect_rate_pct") > 10, "MONITOR")
             .otherwise("OK")
        )
        .withColumn("reported_at", F.current_timestamp())
    )

    gold_metrics.write.format("delta").mode("overwrite").saveAsTable("gold_quality_metrics")

    print("Gold layer ready.")
    gold_metrics.orderBy("defect_rate_pct", ascending=False).show(truncate=False)
    return gold_metrics


if __name__ == "__main__":
    build_gold_metrics(spark)

# =============================================================================
# Silver Layer — Cleaning & Validation
# Manufacturing Quality Pipeline
# =============================================================================
# Purpose : Promote Bronze data to Silver by applying data quality rules.
#           Removes unprocessable records, casts types, adds quality flags.
# Inputs  : bronze_batch, bronze_sensors (Delta tables)
# Outputs : silver_batch, silver_sensors (Delta tables)
#
# Data quality rules applied:
#   Batch  : Drop NULL quality_score | Cast timestamp | Add quality_flag
#   Sensors: Drop NULL temperature   | Filter spikes > 400° | Add temp_flag
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def transform_silver_batch(spark):
    """
    Cleans raw batch manufacturing records.

    Removes rows with NULL quality scores (unprocessable — no metric to evaluate).
    Preserves valid FAILs — a low score is real signal, not bad data.
    Adds quality_flag to stratify HIGH / MEDIUM / LOW performers.
    """
    bronze = spark.table("bronze_batch")
    bronze_count = bronze.count()

    silver_batch = (
        bronze
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .filter(F.col("quality_score").isNotNull())
        .withColumn(
            "quality_flag",
            F.when(F.col("quality_score") < 70, "LOW")
             .when(F.col("quality_score") < 90, "MEDIUM")
             .otherwise("HIGH")
        )
        .withColumn("processed_at", F.current_timestamp())
        .drop("source")
    )

    silver_batch.write.format("delta").mode("overwrite").saveAsTable("silver_batch")

    silver_count = silver_batch.count()
    print(f"Silver batch: {silver_count} rows retained, {bronze_count - silver_count} dropped (NULL quality_score)")
    return silver_batch


def transform_silver_sensors(spark):
    """
    Cleans raw sensor temperature readings.

    Removes NULL readings (sensor offline) and spike anomalies above 400 degrees.
    Adds temp_flag to surface abnormal readings for downstream alerting.
    """
    bronze = spark.table("bronze_sensors")
    bronze_count = bronze.count()

    silver_sensors = (
        bronze
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .filter(F.col("temperature").isNotNull())
        .filter(F.col("temperature") < 400)
        .withColumn(
            "temp_flag",
            F.when(F.col("temperature") > 300, "ABNORMAL")
             .otherwise("NORMAL")
        )
        .withColumn("processed_at", F.current_timestamp())
        .drop("source")
    )

    silver_sensors.write.format("delta").mode("overwrite").saveAsTable("silver_sensors")

    silver_count = silver_sensors.count()
    print(f"Silver sensors: {silver_count} rows retained, {bronze_count - silver_count} dropped (NULL or spike)")
    return silver_sensors


if __name__ == "__main__":
    transform_silver_batch(spark)
    transform_silver_sensors(spark)

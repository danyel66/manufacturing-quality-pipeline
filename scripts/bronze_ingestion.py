# =============================================================================
# Bronze Layer — Raw Ingestion
# Manufacturing Quality Pipeline
# =============================================================================
# Purpose : Land raw batch and sensor data into Delta tables with no
#           transformations. Bronze is the audit trail — never modified.
# Inputs  : mercer_raw_batch, mercer_raw_sensors (or file paths in production)
# Outputs : bronze_batch, bronze_sensors (Delta tables)
# =============================================================================

from pyspark.sql import functions as F


def ingest_bronze_batch(spark):
    """
    Ingests raw batch manufacturing records into the Bronze Delta table.
    Adds ingestion metadata but applies zero transformations.
    """
    bronze_batch = (
        spark.table("raw_batch_records")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit("batch_csv"))
    )

    bronze_batch.write.format("delta").mode("overwrite").saveAsTable("bronze_batch")

    print(f"Bronze batch ingested: {bronze_batch.count()} rows")
    return bronze_batch


def ingest_bronze_sensors(spark):
    """
    Ingests raw sensor stream readings into the Bronze Delta table.
    Preserves all records including nulls and spikes for full audit trail.
    """
    bronze_sensors = (
        spark.table("raw_sensor_readings")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit("sensor_stream"))
    )

    bronze_sensors.write.format("delta").mode("overwrite").saveAsTable("bronze_sensors")

    print(f"Bronze sensors ingested: {bronze_sensors.count()} rows")
    return bronze_sensors


def verify_transaction_log(spark, table_name):
    """
    Prints Delta transaction history to confirm ACID write completed.
    """
    print(f"\nTransaction log for {table_name}:")
    spark.sql(f"DESCRIBE HISTORY {table_name}").show(5, truncate=False)


if __name__ == "__main__":
    ingest_bronze_batch(spark)
    ingest_bronze_sensors(spark)
    verify_transaction_log(spark, "bronze_batch")
    verify_transaction_log(spark, "bronze_sensors")

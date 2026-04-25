# =============================================================================
# Delivery Layer — JDBC Write to MySQL
# Manufacturing Quality Pipeline
# =============================================================================
# Purpose : Write Gold quality metrics to MySQL for ops reporting.
#           Credentials are externalized via Databricks secrets (never hardcoded).
# Inputs  : gold_quality_metrics (Delta table)
# Outputs : MySQL table — quality_metrics
#
# Production pattern:
#   Credentials stored in Databricks secret scope, retrieved at runtime.
#   Connection string uses SSL for encrypted transit.
# =============================================================================

from pyspark.sql import functions as F


def deliver_to_mysql(spark, jdbc_url: str, user: str, password: str):
    """
    Writes Gold quality metrics to MySQL via JDBC.

    Uses overwrite mode so ops always sees the latest run's metrics.
    For append-only audit tables, change mode to 'append' and add a run_id column.

    Args:
        spark    : Active SparkSession
        jdbc_url : JDBC connection string e.g. jdbc:mysql://host:3306/manufacturing_db
        user     : MySQL username (retrieve from Databricks secrets in production)
        password : MySQL password (retrieve from Databricks secrets in production)
    """
    gold_df = spark.table("gold_quality_metrics")

    (
        gold_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "quality_metrics")
        .option("user", user)
        .option("password", password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"Delivered {gold_df.count()} rows to MySQL: quality_metrics")


def deliver_to_delta_sink(spark):
    """
    Fallback delivery for environments without a live MySQL connection.
    Writes Gold to a Delta sink table — identical pattern, different format.
    Used in Databricks Community Edition and CI/CD test runs.
    """
    gold_df = spark.table("gold_quality_metrics")

    gold_df.write.format("delta").mode("overwrite").saveAsTable("delivery_quality_metrics")

    print("Delivered to Delta sink (Community Edition / test mode).")
    spark.sql("""
        SELECT
            process_stage,
            total_valves,
            passed,
            failed,
            avg_quality_score,
            defect_rate_pct,
            status_flag
        FROM delivery_quality_metrics
        ORDER BY defect_rate_pct DESC
    """).show(truncate=False)


if __name__ == "__main__":
    # --- Production ---
    # Retrieve credentials from Databricks secret scope (never hardcode)
    # jdbc_url = "jdbc:mysql://your-host:3306/manufacturing_db"
    # user     = dbutils.secrets.get(scope="manufacturing", key="mysql-user")
    # password = dbutils.secrets.get(scope="manufacturing", key="mysql-password")
    # deliver_to_mysql(spark, jdbc_url, user, password)

    # --- Community Edition / local testing ---
    deliver_to_delta_sink(spark)

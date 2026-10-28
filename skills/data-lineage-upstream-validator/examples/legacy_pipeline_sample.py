"""
Sample Legacy PySpark Transformation Pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

def run_customer_enrichment():
    spark = SparkSession.builder.appName("legacy-spark-customer-enrichment").getOrCreate()

    # Read from upstream sources
    df_customers = spark.table("bronze_erp_customers.raw_customers")
    df_orders = spark.read.table("silver_orders_cleansed.orders_v2")
    df_country_risk = spark.read.table("bronze_risk_lake.country_risk_matrix")

    # Business transformations
    df_enriched = df_customers.join(
        df_orders,
        df_customers.customer_id == df_orders.customer_id,
        "left"
    ).join(
        df_country_risk,
        df_customers.country_code == df_country_risk.country_iso,
        "left"
    ).select(
        df_customers["customer_id"],
        df_customers["customer_name"],
        df_orders["order_id"],
        df_orders["order_amount"],
        df_country_risk["risk_tier"],
        current_timestamp().alias("processed_at")
    )

    # Write target table
    df_enriched.write.mode("overwrite").saveAsTable("gold_customer_analytics.dim_customer_enriched")

if __name__ == "__main__":
    run_customer_enrichment()

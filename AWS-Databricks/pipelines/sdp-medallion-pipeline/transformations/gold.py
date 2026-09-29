from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# Gold layer — materialized views for business reporting
#
# Materialized views use spark.read (batch semantics). SDP's
# incremental refresh engine processes only new or changed data
# from Silver whenever possible.
#
# Gold reads Silver's CURRENT state — records where __END_AT IS NULL.
# This is the SCD Type 2 current-record filter. Historical rows
# are excluded unless the business requirement is point-in-time.
#
# Never use spark.readStream in a @dp.materialized_view function.
# ─────────────────────────────────────────────────────────────────


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Daily revenue by customer tier — current orders only (SCD Type 2 open records)"
)
def gold_daily_revenue():
    orders = (
        spark.read.table("silver_orders")
            .filter(F.col("__END_AT").isNull())          # current records only
            .filter(F.col("status") == "completed")      # completed orders only
    )

    customers = (
        spark.read.table("silver_customers")
            .filter(F.col("__END_AT").isNull())          # current records only
            .select("customer_id", "customer_tier")
    )
    return(
        orders
            .join(customers, on="customer_id", how="left")
            .groupBy(
                F.to_date(F.col("order_date")).alias("order_date"),
                F.col("customer_tier")
            )
            .agg(
                F.sum("amount").alias("total_revenue"),
                F.count("order_id").alias("order_count"),
                F.avg("amount").alias("avg_order_value"),
            )
            .orderBy("order_date", "customer_tier")
    )


from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ─────────────────────────────────────────────────────────────────
# Bronze layer — raw CDC event landing
#
# Auto Loader reads Debezium-flattened CDC files from the landing
# volume and appends them to streaming tables exactly as received.
# No filtering, no transformation. Every event — INSERT, UPDATE,
# DELETE — lands here with Bronze metadata columns attached.
#
# Bronze is append-only. It is the immutable audit trail of every
# CDC event that arrived. Silver applies the SCD logic.
#
# Debezium flattened format fields:
#   op        — operation: 'c' (create), 'u' (update), 'd' (delete)
#   ts_ms     — Debezium processing timestamp, epoch milliseconds
#   __deleted — 'true' for delete tombstones (Debezium SMT)
#   (business columns from the 'after' field)
# ─────────────────────────────────────────────────────────────────

ORDERS_LANDING_PATH    = "/Volumes/dev/dbx_course/landing/orders/"
CUSTOMERS_LANDING_PATH = "/Volumes/dev/dbx_course/landing/customers/"
SCHEMA_LOCATION_BASE   = "/Volumes/dev/dbx_course/landing/_schema"

@dp.table(name="bronze_orders_cdc", 
          comment="Raw Debezium CDC events for orders — append-only landing table")
@dp.expect("op_is_valid", "op IN ('c', 'u', 'd')")
@dp.expect_or_drop("ts_ms_not_null", "ts_ms IS NOT NULL")
def bronze_orders_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    SCHEMA_LOCATION_BASE + "/orders")
            .option("header", "true")
            .load(ORDERS_LANDING_PATH)
            .select("*",
                    col("_metadata.file_modification_time").alias("_ingest_timestamp"),
                    col("_metadata.file_path").alias("_source_file")
            )
    )


@dp.table(name="bronze_customers_cdc",
          comment="Raw Debezium CDC events for customers — append-only landing table")
@dp.expect("op_is_valid", "op IN ('c', 'u', 'd')")
@dp.expect_or_drop("ts_ms_not_null", "ts_ms IS NOT NULL")
def bronze_customers_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation",
                    SCHEMA_LOCATION_BASE + "/customers")
            .option("header", "true")
            .load(CUSTOMERS_LANDING_PATH)
            .select(
                "*",
                col("_metadata.file_modification_time").alias("_ingest_timestamp"),
                col("_metadata.file_path").alias("_source_file"),
            )
    )


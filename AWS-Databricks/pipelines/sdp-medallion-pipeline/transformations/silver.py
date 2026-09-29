from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# Silver layer — AUTO CDC with SCD Type 2
#
# AUTO CDC reads CDC events from Bronze streaming tables and applies
# them to Silver streaming tables using SCD Type 2 history tracking.
#
# SCD Type 2 adds __START_AT and __END_AT columns automatically:
#   __END_AT IS NULL  → current version of the record
#   __END_AT IS NOT NULL → historical version (superseded)
#
# AUTO CDC handles out-of-order events via sequence_by = ts_ms.
# Deletes are applied when op = 'd' OR __deleted = 'true'
# (covering both Debezium op field and flattening SMT convention).
#
# Never write to Silver directly with @dp.table — AUTO CDC is the
# exclusive flow into Silver. create_streaming_table() declares the
# target; create_auto_cdc_flow() defines how it gets populated.
# ─────────────────────────────────────────────────────────────────

# ── Silver Orders ─────────────────────────────────────────────────

dp.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 2 history of order CDC events from Bronze",
    expect_all_or_drop={
        "valid_order_id": "order_id IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_amount":      "amount > 0"
    }
)

dp.create_auto_cdc_flow(
    target = "silver_orders",
    source = "bronze_orders_cdc",
    keys = ["order_id"],
    sequence_by = "ts_ms",
    apply_as_deletes = "op = 'd' or __deleted = 'true'",
    stored_as_scd_type = "2",
    except_column_list = ["_ingest_timestamp", "_source_file",
                          "__deleted", "op", "ts_ms"]
)


# ── Silver Customers ──────────────────────────────────────────────

dp.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 history of customer CDC events from Bronze",
    expect_all_or_drop={
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_name":        "customer_name IS NOT NULL",
    }
)

dp.create_auto_cdc_flow(
    target      = "silver_customers",
    source      = "bronze_customers_cdc",
    keys        = ["customer_id"],
    sequence_by = "ts_ms",
    apply_as_deletes   = "op = 'd' OR __deleted = 'true'",
    stored_as_scd_type = "2",
    except_column_list = ["_ingest_timestamp", "_source_file",
                          "__deleted", "op", "ts_ms"],
)




















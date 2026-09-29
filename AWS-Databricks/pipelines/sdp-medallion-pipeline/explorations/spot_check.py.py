# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
display(spark.table("dev.dbx_course.bronze_orders_cdc"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.dbx_course.bronze_orders_cdc

# dq_report.py
# ============================================================
# Reporting script
#
# Python script task reads job parameters as sys.argv.
# Job config maps: arg[1] = run_date
# via dynamic value reference: ["{{job.parameters.run_date}}"]
# ============================================================

import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: dq_report.py <run_date>")

    run_date = sys.argv[1]

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    # Read taskValue from upstream DQ check
    try:
        dq_row_count = dbutils.jobs.taskValues.get(
            taskKey="dq_check_gold",
            key="row_count",
            debugValue=0   # returned when running outside a job context
        )
    except Exception:
        dq_row_count = 0

    # Skip the report if no data found. 
    # This might happen in debug mode or error in reading task vale 
    if dq_row_count == 0:
        print(f"No data found for {run_date}. Report skipped.")
        return        

    # Generate the report
    print(f"Generating report for run_date={run_date}")
    table = "dev.dbx_course.gold_daily_revenue"  

    df = spark.table(table).filter(f"order_date = '{run_date}'")
    total = df.agg({"total_revenue": "sum"}).collect()[0][0]
    
    print(f"Date          : {run_date}")
    print(f"Rows           : {dq_row_count}")
    print(f"Total revenue  : {total:,.2f}")
    print("Report complete.")

if __name__ == "__main__":
    main()
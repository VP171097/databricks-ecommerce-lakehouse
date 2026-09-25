# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Data quality report
# MAGIC Turns pipeline expectation metrics from the event log into `ecom_ops.dq_summary`.

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "")
PIPELINE_ID = dbutils.widgets.get("pipeline_id")
assert PIPELINE_ID, "Pass the pipeline_id parameter"
OPS = "portfolio.ecom_ops"

expectations = spark.sql(f"""
  SELECT origin.update_id AS update_id,
         timestamp        AS run_at,
         explode(from_json(details:flow_progress.data_quality.expectations,
           'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
         )) AS e
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
""")
expectations.createOrReplaceTempView("expectations")

summary = spark.sql("""
  SELECT update_id, e.dataset AS table_name, e.name AS rule_name,
         SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed,
         ROUND(SUM(e.failed_records) / NULLIF(SUM(e.passed_records + e.failed_records), 0), 4)
           AS failed_pct,
         MAX(run_at) AS run_at
  FROM expectations
  GROUP BY update_id, e.dataset, e.name
""")
summary.createOrReplaceTempView("summary")
display(summary.orderBy("failed_pct", ascending=False))

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {OPS}.dq_summary (
    update_id STRING, table_name STRING, rule_name STRING, passed BIGINT, failed BIGINT,
    failed_pct DOUBLE, run_at TIMESTAMP)
""")
spark.sql(f"""
  MERGE INTO {OPS}.dq_summary t
  USING summary s
  ON t.update_id = s.update_id AND t.table_name = s.table_name AND t.rule_name = s.rule_name
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

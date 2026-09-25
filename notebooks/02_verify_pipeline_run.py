# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Verify pipeline run
# MAGIC Automated checks after every run. Fails the task if any check fails.

# COMMAND ----------

from datetime import UTC, datetime

from pyspark.sql import functions as F

dbutils.widgets.text("run_id", "manual")
RUN_ID = dbutils.widgets.get("run_id")
CAT = "portfolio"
B, S, G, OPS = f"{CAT}.ecom_bronze", f"{CAT}.ecom_silver", f"{CAT}.ecom_gold", f"{CAT}.ecom_ops"
results = []


def check(name, passed, detail=""):
    results.append((name, bool(passed), str(detail)))


def count(table):
    return spark.table(table).count()


# COMMAND ----------

# Reconciliation: bronze = silver + quarantine
for t in ["orders", "order_items"]:
    b, s, q = count(f"{B}.{t}"), count(f"{S}.{t}"), count(f"{S}.{t}_quarantine")
    check(f"reconcile_{t}", b == s + q, f"bronze={b} silver={s} quarantine={q}")

# Uniqueness of business keys
for t, k in {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
}.items():
    dups = spark.table(f"{S}.{t}").groupBy(*k).count().where("count > 1").count()
    check(f"unique_{t}", dups == 0, f"duplicate keys={dups}")

# One current row per customer in the SCD2 dimension
multi_current = (
    spark.table(f"{G}.dim_customer")
    .where("__END_AT IS NULL")
    .groupBy("customer_unique_id")
    .count()
    .where("count > 1")
    .count()
)
check("one_current_customer_row", multi_current == 0, f"customers={multi_current}")

# Referential integrity
fact = spark.table(f"{G}.fact_order_item")
for dim, key in [("dim_product", "product_id"), ("dim_seller", "seller_id")]:
    missing = fact.join(spark.table(f"{G}.{dim}"), key, "left_anti").count()
    check(f"fk_{key}", missing == 0, f"missing={missing}")

# Freshness
latest = spark.table(f"{B}.orders").agg(F.max("_ingested_at")).collect()[0][0]
age_h = (datetime.now(UTC) - latest.replace(tzinfo=UTC)).total_seconds() / 3600 if latest else None
check("freshness_orders", age_h is not None and age_h < 24, f"hours={age_h}")

# Totals
gold_rev = spark.table(f"{G}.agg_daily_sales").agg(F.sum("revenue")).collect()[0][0] or 0
silver_rev = (
    spark.table(f"{S}.order_items")
    .join(spark.table(f"{S}.orders"), "order_id")
    .where("order_status = 'delivered'")
    .agg(F.sum("price"))
    .collect()[0][0]
    or 0
)
check(
    "revenue_gold_equals_silver",
    abs(float(gold_rev) - float(silver_rev)) < 0.01,
    f"gold={gold_rev} silver={silver_rev}",
)

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS {OPS}.run_checks "
    "(run_id STRING, check_name STRING, passed BOOLEAN, detail STRING, checked_at TIMESTAMP)"
)
df = (
    spark.createDataFrame(results, "check_name string, passed boolean, detail string")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("checked_at", F.current_timestamp())
    .select("run_id", "check_name", "passed", "detail", "checked_at")
)
df.write.mode("append").saveAsTable(f"{OPS}.run_checks")
display(df)

failed = [r[0] for r in results if not r[1]]
if failed:
    raise AssertionError(f"Checks failed: {failed}")

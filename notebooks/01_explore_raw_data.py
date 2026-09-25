# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Explore raw data
# MAGIC Profile the Olist files before designing the pipeline. Upload the 9 raw CSV files to
# MAGIC `/Volumes/portfolio/ecom_landing/profiling/` first (see manual A10).

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/portfolio/ecom_landing/profiling"
FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def load(name):
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"{BASE}/{FILES[name]}")
    )


dfs = {name: load(name) for name in FILES}
for name, df in dfs.items():
    print(f"{name:<22} rows={df.count():>9,}  cols={len(df.columns)}")

# COMMAND ----------

# Nulls and empty strings per column, sorted by percentage


def null_profile(df):
    total = df.count()
    exprs = [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    row = df.agg(*exprs).collect()[0].asDict()
    rows = [(c, n, round(100 * n / total, 2)) for c, n in row.items()]
    return spark.createDataFrame(rows, "column string, nulls long, pct double").orderBy(
        F.desc("pct")
    )


display(null_profile(dfs["orders"]))

# COMMAND ----------

# Key uniqueness
keys = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_reviews": ["review_id", "order_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
}
for name, k in keys.items():
    df = dfs[name]
    print(f"{name:<15} rows={df.count():>7}  distinct keys={df.select(*k).distinct().count():>7}")
display(dfs["order_reviews"].groupBy("review_id").count().where("count > 1").limit(10))

# COMMAND ----------

# Dates and numbers
o = dfs["orders"].select(F.to_timestamp("order_purchase_timestamp").alias("ts"))
display(o.agg(F.min("ts"), F.max("ts"), F.sum(F.col("ts").isNull().cast("int")).alias("bad")))
i = dfs["order_items"].select(
    F.col("price").cast("double").alias("price"),
    F.col("freight_value").cast("double").alias("freight"),
)
display(i.summary("min", "max", "mean"))

# COMMAND ----------

# Categoricals and relationships
display(dfs["orders"].groupBy("order_status").count().orderBy(F.desc("count")))
display(dfs["order_payments"].groupBy("payment_type").count())
orphans = dfs["order_items"].join(dfs["orders"], "order_id", "left_anti").count()
print("order_items without an order:", orphans)
multiline = dfs["order_reviews"].where(F.col("review_comment_message").contains("\n")).count()
print("reviews with line breaks:", multiline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings
# MAGIC - Duplicate `review_id` values exist: dedupe on (review_id, order_id).
# MAGIC - Product columns are misspelled (`product_name_lenght`): rename in silver.
# MAGIC - Geolocation has many rows per zip prefix: aggregate to one row.
# MAGIC - Review comments contain line breaks: bronze needs multi-line CSV parsing.
# MAGIC - (add your own findings here)

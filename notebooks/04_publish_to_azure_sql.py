# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Publish to Azure SQL (Azure track)
# MAGIC Writes gold tables to `servingdb` for applications. Secrets come from the
# MAGIC Key Vault-backed scope `kv-portfolio`.

# COMMAND ----------

from datetime import UTC, datetime

dbutils.widgets.text("tables", "agg_daily_sales,agg_seller_performance")
dbutils.widgets.text("database", "servingdb")
TABLES = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
DATABASE = dbutils.widgets.get("database")

host = dbutils.secrets.get("kv-portfolio", "sql-host")
props = {
    "user": dbutils.secrets.get("kv-portfolio", "sql-user"),
    "password": dbutils.secrets.get("kv-portfolio", "sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}
url = (
    f"jdbc:sqlserver://{host}:1433;database={DATABASE};encrypt=true;"
    "trustServerCertificate=false;loginTimeout=30"
)

# COMMAND ----------

log = []
for table in TABLES:
    df = spark.table(f"portfolio.ecom_gold.{table}")
    for name, dtype in df.dtypes:
        if dtype.startswith("decimal"):
            df = df.withColumn(name, df[name].cast("decimal(12,2)"))
        elif dtype == "double":
            df = df.withColumn(name, df[name].cast("decimal(18,4)"))
    expected = df.count()
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", f"dbo.{table}")
        .option("truncate", "true")
        .option("batchsize", 10000)
        .options(**props)
        .mode("overwrite")
        .save()
    )
    actual = (
        spark.read.format("jdbc")
        .option("url", url)
        .options(**props)
        .option("query", f"SELECT COUNT(*) AS n FROM dbo.{table}")
        .load()
        .collect()[0]["n"]
    )
    print(f"{table}: gold={expected} sql={actual}")
    if actual != expected:
        raise AssertionError(f"Row count mismatch for {table}")
    log.append((table, expected, datetime.now(UTC)))

(
    spark.createDataFrame(log, "table_name string, rows long, published_at timestamp")
    .write.mode("append")
    .saveAsTable("portfolio.ecom_ops.publish_log")
)

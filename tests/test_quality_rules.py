from pyspark.sql import functions as F

from ecom.transformations.quality_rules import RULES, failed_rules_column, quarantine_filter


def test_quarantine_selects_row_once_with_all_failed_rules(spark):
    rules = RULES["orders"]
    df = spark.createDataFrame(
        [
            ("o1", "c1", "delivered", "2017-01-01 00:00:00"),
            (None, "c2", "lost", "2017-01-01 00:00:00"),
        ],
        "order_id string, customer_id string, order_status string, ts string",
    ).withColumn("purchased_at", F.to_timestamp("ts"))
    bad = df.where(quarantine_filter(rules)).withColumn("failed", failed_rules_column(rules))
    rows = bad.collect()
    assert len(rows) == 1
    assert set(rows[0].failed) == {"valid_order_id", "valid_status"}


def test_null_counts_as_failure(spark):
    rules = RULES["order_items"]
    df = spark.createDataFrame(
        [("o1", 1, "p1", None, 1.0)],
        "order_id string, order_item_id int, product_id string, price double, freight_value double",
    )
    assert df.where(quarantine_filter(rules)).count() == 1

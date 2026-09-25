"""Gold: star schema and business aggregates."""

import os
import sys

try:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
except NameError:
    pass

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ecom import config as C
from ecom import pipelines_compat as dp

spark = SparkSession.getActiveSession()


def silver(name):
    return spark.read.table(C.fq("silver", name))


def gold(name):
    return spark.read.table(C.fq("gold", name))


# ---- dim_customer: SCD Type 2 from the change feed
dp.create_streaming_table(name=C.fq("gold", "dim_customer"), comment="Customers, SCD Type 2")
dp.create_auto_cdc_flow(
    target=C.fq("gold", "dim_customer"),
    source=C.fq("silver", "customer_changes"),
    keys=["customer_unique_id"],
    sequence_by=F.col("change_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["zip_code_prefix", "city", "state"],
)


@dp.materialized_view(name=C.fq("gold", "dim_product"), comment="Product dimension")
def dim_product():
    volume = F.col("length_cm") * F.col("height_cm") * F.col("width_cm")
    return silver("products").select(
        "product_id",
        "category_name_en",
        "category_name_pt",
        "weight_g",
        F.round(volume, 1).alias("volume_cm3"),
        F.when(volume.isNull(), "unknown")
        .when(volume < 2000, "small")
        .when(volume < 20000, "medium")
        .otherwise("large")
        .alias("size_band"),
    )


@dp.materialized_view(name=C.fq("gold", "dim_seller"), comment="Seller dimension")
def dim_seller():
    geo = silver("geolocation").select("zip_code_prefix", "lat", "lng")
    return (
        silver("sellers")
        .join(geo, "zip_code_prefix", "left")
        .select("seller_id", "city", "state", "lat", "lng")
    )


@dp.materialized_view(name=C.fq("gold", "dim_date"), comment="Calendar 2016-2018")
def dim_date():
    days = spark.sql(
        "SELECT explode(sequence(DATE'2016-01-01', DATE'2018-12-31', INTERVAL 1 DAY)) AS date"
    )
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


@dp.materialized_view(name=C.fq("gold", "fact_order_item"), comment="One row per order item")
def fact_order_item():
    orders = silver("orders").select(
        "order_id",
        "customer_id",
        "order_status",
        F.col("purchase_date").alias("order_date"),
        "delivery_days",
        "delivered_late",
    )
    customers = silver("customers").select("customer_id", "customer_unique_id")
    reviews = (
        silver("order_reviews")
        .groupBy("order_id")
        .agg(F.round(F.avg("review_score"), 1).alias("review_score"))
    )
    return (
        silver("order_items")
        .join(orders, "order_id")
        .join(customers, "customer_id")
        .join(reviews, "order_id", "left")
        .select(
            "order_id",
            "order_item_id",
            "customer_unique_id",
            "product_id",
            "seller_id",
            "order_date",
            "order_status",
            "price",
            "freight_value",
            "item_total",
            "delivery_days",
            "delivered_late",
            "review_score",
        )
    )


@dp.materialized_view(name=C.fq("gold", "fact_payment"), comment="One row per payment")
def fact_payment():
    orders = silver("orders").select("order_id", F.col("purchase_date").alias("order_date"))
    return (
        silver("order_payments")
        .join(orders, "order_id")
        .select(
            "order_id",
            "payment_sequential",
            "order_date",
            "payment_type",
            F.col("payment_installments").alias("installments"),
            "payment_value",
        )
    )


@dp.materialized_view(name=C.fq("gold", "agg_daily_sales"), comment="Delivered sales per day")
def agg_daily_sales():
    f = gold("fact_order_item").where("order_status = 'delivered'")
    late_order = F.when(F.col("delivered_late"), F.col("order_id"))
    daily = f.groupBy("order_date").agg(
        F.count_distinct("order_id").alias("orders"),
        F.count(F.lit(1)).alias("items"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.sum("freight_value"), 2).alias("freight"),
        F.count_distinct(late_order).alias("late_orders"),
    )
    return daily.select(
        "order_date",
        "orders",
        "items",
        "revenue",
        "freight",
        F.round(F.col("revenue") / F.col("orders"), 2).alias("avg_order_value"),
        F.round(F.col("late_orders") / F.col("orders"), 4).alias("late_delivery_rate"),
    )


@dp.materialized_view(name=C.fq("gold", "agg_category_monthly"))
def agg_category_monthly():
    f = gold("fact_order_item").join(gold("dim_product"), "product_id")
    return f.groupBy(
        F.date_format("order_date", "yyyy-MM").alias("year_month"), "category_name_en"
    ).agg(
        F.count_distinct("order_id").alias("orders"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
    )


@dp.materialized_view(name=C.fq("gold", "agg_seller_performance"))
def agg_seller_performance():
    late_order = F.when(F.col("delivered_late"), F.col("order_id"))
    per_seller = (
        gold("fact_order_item")
        .groupBy("seller_id")
        .agg(
            F.count_distinct("order_id").alias("orders"),
            F.round(F.sum("price"), 2).alias("revenue"),
            F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
            F.count_distinct(late_order).alias("late_orders"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
    )
    ranked = per_seller.withColumn(
        "rank_by_revenue", F.rank().over(Window.orderBy(F.col("revenue").desc()))
    )
    return ranked.join(gold("dim_seller").select("seller_id", "state"), "seller_id", "left").select(
        "seller_id",
        "state",
        "orders",
        "revenue",
        "avg_delivery_days",
        F.round(F.col("late_orders") / F.col("orders"), 4).alias("late_rate"),
        "avg_review_score",
        "rank_by_revenue",
    )


@dp.materialized_view(name=C.fq("gold", "agg_customer_cohorts"), comment="Monthly retention")
def agg_customer_cohorts():
    f = (
        gold("fact_order_item")
        .select("customer_unique_id", F.trunc("order_date", "month").alias("order_month"))
        .distinct()
    )
    first = f.groupBy("customer_unique_id").agg(F.min("order_month").alias("cohort_month"))
    j = f.join(first, "customer_unique_id").withColumn(
        "months_since_first",
        F.months_between("order_month", "cohort_month").cast("int"),
    )
    active = j.groupBy("cohort_month", "months_since_first").agg(
        F.count_distinct("customer_unique_id").alias("active_customers")
    )
    size = active.where("months_since_first = 0").select(
        "cohort_month", F.col("active_customers").alias("cohort_size")
    )
    return active.join(size, "cohort_month").withColumn(
        "retention_rate", F.round(F.col("active_customers") / F.col("cohort_size"), 4)
    )

"""Silver typing for customers and the change feed that drives the SCD2 dimension."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom.transformations.cleaning import (
    empty_to_null,
    pad_zip,
    parse_ts,
    tech_cols,
    title_case,
    trim_all_strings,
    upper_case,
)


def type_customers(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    return df.select(
        "customer_id",
        "customer_unique_id",
        pad_zip("customer_zip_code_prefix").alias("zip_code_prefix"),
        title_case("customer_city").alias("city"),
        upper_case("customer_state").alias("state"),
        *tech_cols(df),
    )


def type_customer_updates(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    return df.select(
        "customer_unique_id",
        pad_zip("customer_zip_code_prefix").alias("zip_code_prefix"),
        title_case("customer_city").alias("city"),
        upper_case("customer_state").alias("state"),
        parse_ts("change_ts").alias("change_ts"),
    )


def build_customer_changes(
    customers: DataFrame, orders: DataFrame, updates: DataFrame
) -> DataFrame:
    """Union of (a) customers as first seen on their order and (b) explicit updates.

    customers: output of type_customers; orders: silver orders (needs customer_id,
    purchased_at); updates: raw bronze customer_updates.
    """
    first_seen = customers.join(
        orders.select("customer_id", F.col("purchased_at").alias("change_ts")), "customer_id"
    ).select(
        "customer_unique_id",
        "zip_code_prefix",
        "city",
        "state",
        "change_ts",
        F.lit("order").alias("change_source"),
    )
    moved = type_customer_updates(updates).withColumn("change_source", F.lit("update"))
    return first_seen.unionByName(moved).where(F.col("change_ts").isNotNull())

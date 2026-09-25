"""Silver typing for payments and reviews."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecom.transformations.cleaning import (
    empty_to_null,
    parse_ts,
    tech_cols,
    trim_all_strings,
    try_cast,
)


def type_payments(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    ptype = F.lower(F.col("payment_type"))
    installments = try_cast("payment_installments", "INT")
    return df.select(
        "order_id",
        try_cast("payment_sequential", "INT").alias("payment_sequential"),
        F.when(ptype == "not_defined", "unknown").otherwise(ptype).alias("payment_type"),
        F.when(installments == 0, 1).otherwise(installments).alias("payment_installments"),
        try_cast("payment_value", "DECIMAL(12,2)").alias("payment_value"),
        *tech_cols(df),
    )


def _clean_text(col: str):
    no_breaks = F.regexp_replace(F.col(col), r"[\r\n]+", " ")
    trimmed = F.trim(no_breaks)
    return F.when(trimmed == "", None).otherwise(trimmed)


def type_reviews(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    return df.select(
        "review_id",
        "order_id",
        try_cast("review_score", "INT").alias("review_score"),
        _clean_text("review_comment_title").alias("review_title"),
        _clean_text("review_comment_message").alias("review_message"),
        parse_ts("review_creation_date").alias("created_at"),
        parse_ts("review_answer_timestamp").alias("answered_at"),
        *tech_cols(df),
    )


def latest_per_key(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep one row per key: the one with the latest order_col (nulls last)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

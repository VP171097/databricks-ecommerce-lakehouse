"""Silver typing for order items."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom.transformations.cleaning import (
    empty_to_null,
    parse_ts,
    tech_cols,
    trim_all_strings,
    try_cast,
)


def type_order_items(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    out = df.select(
        "order_id",
        try_cast("order_item_id", "INT").alias("order_item_id"),
        "product_id",
        "seller_id",
        parse_ts("shipping_limit_date").alias("shipping_limit_at"),
        try_cast("price", "DECIMAL(12,2)").alias("price"),
        try_cast("freight_value", "DECIMAL(12,2)").alias("freight_value"),
        *tech_cols(df),
    )
    return out.withColumn(
        "item_total", (F.col("price") + F.col("freight_value")).cast("decimal(12,2)")
    )

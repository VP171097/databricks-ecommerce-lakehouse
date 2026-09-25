"""Silver typing for orders."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom.transformations.cleaning import empty_to_null, parse_ts, tech_cols, trim_all_strings

TS_COLUMNS = {
    "order_purchase_timestamp": "purchased_at",
    "order_approved_at": "approved_at",
    "order_delivered_carrier_date": "delivered_carrier_at",
    "order_delivered_customer_date": "delivered_customer_at",
    "order_estimated_delivery_date": "estimated_delivery_at",
}


def type_orders(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    cols = [
        F.col("order_id"),
        F.col("customer_id"),
        F.lower(F.col("order_status")).alias("order_status"),
    ]
    cols += [parse_ts(src).alias(dst) for src, dst in TS_COLUMNS.items()]
    out = df.select(*cols, *tech_cols(df))
    seconds = F.unix_timestamp("delivered_customer_at") - F.unix_timestamp("purchased_at")
    return (
        out.withColumn("purchase_date", F.to_date("purchased_at"))
        .withColumn("is_delivered", F.col("order_status") == F.lit("delivered"))
        .withColumn("delivery_days", F.floor(seconds / 86400).cast("int"))
        .withColumn(
            "delivered_late",
            F.when(F.col("delivered_customer_at").isNull(), F.lit(None).cast("boolean")).otherwise(
                F.to_date("delivered_customer_at") > F.to_date("estimated_delivery_at")
            ),
        )
    )

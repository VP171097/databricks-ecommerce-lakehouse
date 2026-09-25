"""Silver: typed, deduplicated, validated tables plus quarantine tables."""

import os
import sys

def _setup_sys_path():
    candidates = []
    if "__file__" in globals() and __file__:
        raw = __file__
        candidates.extend([
            raw,
            f"/Workspace/Users/{raw.lstrip('/')}",
            f"/Workspace/{raw.lstrip('/')}",
        ])
    cwd = os.getcwd()
    candidates.extend([cwd, f"/Workspace/{cwd.lstrip('/')}"])

    for candidate in candidates:
        curr = candidate
        for _ in range(7):
            src_dir = os.path.join(curr, "src")
            if os.path.isdir(src_dir) and src_dir not in sys.path:
                sys.path.insert(0, src_dir)
                return
            if os.path.isdir(os.path.join(curr, "ecom")) and curr not in sys.path:
                sys.path.insert(0, curr)
                return
            parent = os.path.dirname(curr)
            if parent == curr:
                break
            curr = parent

_setup_sys_path()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom import config as C
from ecom import pipelines_compat as dp
from ecom.transformations.customers import build_customer_changes, type_customers
from ecom.transformations.order_items import type_order_items
from ecom.transformations.orders import type_orders
from ecom.transformations.payments_reviews import latest_per_key, type_payments, type_reviews
from ecom.transformations.products_sellers import (
    aggregate_geolocation,
    type_categories,
    type_products,
    type_sellers,
)
from ecom.transformations.quality_rules import (
    RULES,
    WARN_RULES,
    failed_rules_column,
    quarantine_filter,
)

spark = SparkSession.getActiveSession()

STREAMING = {
    "orders": type_orders,
    "order_items": type_order_items,
    "order_payments": type_payments,
    "customers": type_customers,
}


def make_silver(name: str, typer) -> None:
    def _silver():
        return (
            typer(spark.readStream.table(C.fq("bronze", name)))
            .withWatermark("_ingested_at", "1 day")
            .dropDuplicatesWithinWatermark(C.KEYS[name])
        )

    dp.table(
        name=C.fq("silver", name),
        comment=f"Typed, deduplicated {name}",
        table_properties={"quality": "silver"},
    )(dp.with_rules(_silver, RULES.get(name), WARN_RULES.get(name)))


def make_quarantine(name: str, typer) -> None:
    rules = RULES[name]

    @dp.table(name=C.fq("silver", f"{name}_quarantine"), comment=f"{name} rows failing rules")
    def _quarantine():
        typed = typer(spark.readStream.table(C.fq("bronze", name)))
        return (
            typed.where(quarantine_filter(rules))
            .withColumn("failed_rules", failed_rules_column(rules))
            .withColumn("quarantined_at", F.current_timestamp())
        )


for _name, _typer in STREAMING.items():
    make_silver(_name, _typer)
    if _name in RULES:
        make_quarantine(_name, _typer)


# Reviews: small, need "latest answer per key" -> materialized views (full recompute)
def _typed_reviews():
    return type_reviews(spark.read.table(C.fq("bronze", "order_reviews")))


dp.materialized_view(name=C.fq("silver", "order_reviews"), comment="Latest review per key")(
    dp.with_rules(
        lambda: latest_per_key(_typed_reviews(), C.KEYS["order_reviews"], "answered_at"),
        RULES["order_reviews"],
    )
)


@dp.materialized_view(name=C.fq("silver", "order_reviews_quarantine"))
def order_reviews_quarantine():
    rules = RULES["order_reviews"]
    return (
        _typed_reviews()
        .where(quarantine_filter(rules))
        .withColumn("failed_rules", failed_rules_column(rules))
        .withColumn("quarantined_at", F.current_timestamp())
    )


@dp.table(name=C.fq("silver", "customer_changes"), comment="Feed for the SCD2 customer dim")
def customer_changes():
    customers = type_customers(spark.readStream.table(C.fq("bronze", "customers")))
    updates = spark.readStream.table(C.fq("bronze", "customer_updates"))
    orders = spark.read.table(C.fq("silver", "orders"))
    return build_customer_changes(customers, orders, updates)


@dp.materialized_view(name=C.fq("silver", "category_translation"))
def category_translation():
    return type_categories(spark.read.table(C.fq("bronze", "category_translation")))


dp.materialized_view(name=C.fq("silver", "products"), comment="Products with English category")(
    dp.with_rules(
        lambda: type_products(
            spark.read.table(C.fq("bronze", "products")),
            spark.read.table(C.fq("bronze", "category_translation")),
        ),
        None,
        WARN_RULES["products"],
    )
)


@dp.materialized_view(name=C.fq("silver", "sellers"))
def sellers():
    return type_sellers(spark.read.table(C.fq("bronze", "sellers")))


@dp.materialized_view(name=C.fq("silver", "geolocation"), comment="One row per zip prefix")
def geolocation():
    return aggregate_geolocation(spark.read.table(C.fq("bronze", "geolocation")))

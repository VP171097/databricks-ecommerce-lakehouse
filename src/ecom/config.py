"""Central names and paths. Nothing else in the repo hard-codes catalog or schema names."""

CATALOG = "portfolio"

SCHEMAS = {
    "landing": "ecom_landing",
    "bronze": "ecom_bronze",
    "silver": "ecom_silver",
    "gold": "ecom_gold",
    "ops": "ecom_ops",
}

LANDING_PATH = f"/Volumes/{CATALOG}/ecom_landing/raw"

SOURCE_FILES = {
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

SOURCES = list(SOURCE_FILES) + ["customer_updates"]

KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "customers": ["customer_id"],
    "customer_updates": ["customer_unique_id", "change_ts"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category_translation": ["product_category_name"],
}

ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
]


def fq(layer: str, table: str) -> str:
    """Fully qualified table name, e.g. fq("silver", "orders") -> portfolio.ecom_silver.orders."""
    return f"{CATALOG}.{SCHEMAS[layer]}.{table}"


def landing(source: str, base: str = LANDING_PATH) -> str:
    """Landing folder of one source inside the Volume."""
    return f"{base}/{source}"

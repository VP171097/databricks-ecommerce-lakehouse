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

# Raw CSV headers, in file order. Bronze reads with this explicit all-STRING schema, so an
# empty or brand-new landing folder never breaks the pipeline (no schema inference needed).
RAW_COLUMNS = {
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    "order_payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "order_reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "customer_updates": [
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "change_ts",
    ],
    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    "category_translation": ["product_category_name", "product_category_name_english"],
}


def bronze_schema(source: str) -> str:
    """DDL schema string with every raw column as STRING."""
    return ", ".join(f"`{c}` STRING" for c in RAW_COLUMNS[source])


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

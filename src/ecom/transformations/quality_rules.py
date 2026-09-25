"""Data quality rules. Conditions are SQL expressions that must be TRUE for a valid row."""

from pyspark.sql import Column
from pyspark.sql import functions as F

from ecom.config import ORDER_STATUSES

_STATUS_LIST = ", ".join(f"'{s}'" for s in ORDER_STATUSES)

RULES: dict[str, dict[str, str]] = {
    "orders": {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_status": f"order_status IN ({_STATUS_LIST})",
        "valid_purchase_ts": "purchased_at IS NOT NULL",
    },
    "order_items": {
        "valid_keys": (
            "order_id IS NOT NULL AND order_item_id IS NOT NULL AND product_id IS NOT NULL"
        ),
        "positive_price": "price > 0",
        "non_negative_freight": "freight_value >= 0",
    },
    "order_reviews": {
        "valid_score": "review_score BETWEEN 1 AND 5",
    },
}

WARN_RULES: dict[str, dict[str, str]] = {
    "orders": {
        "delivery_after_purchase": (
            "delivered_customer_at IS NULL OR delivered_customer_at >= purchased_at"
        ),
    },
    "order_payments": {"non_negative_payment": "payment_value >= 0"},
    "customers": {"valid_state": "length(state) = 2"},
    "products": {"known_category": "category_name_pt <> 'uncategorized'"},
}


def quarantine_filter(rules: dict[str, str]) -> str:
    """SQL condition selecting rows that fail at least one rule (null counts as a failure)."""
    return " OR ".join(f"NOT coalesce(({cond}), false)" for cond in rules.values())


def failed_rules_column(rules: dict[str, str]) -> Column:
    """Array with the names of the rules each row fails."""
    flags = [
        F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules.items()
    ]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())

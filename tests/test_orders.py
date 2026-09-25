from ecom.transformations.orders import type_orders

BASE = {
    "order_id": "o1",
    "customer_id": "c1",
    "order_status": "Delivered",
    "order_purchase_timestamp": "2017-01-01 10:00:00",
    "order_approved_at": "",
    "order_delivered_carrier_date": "",
    "order_delivered_customer_date": "2017-01-05 09:00:00",
    "order_estimated_delivery_date": "2017-01-04 00:00:00",
}


def test_delivery_days_and_late(df_from):
    row = type_orders(df_from([BASE])).collect()[0]
    assert row.delivery_days == 3
    assert row.delivered_late is True
    assert row.order_status == "delivered"
    assert row.approved_at is None


def test_not_delivered_gives_null_late(df_from):
    rec = dict(BASE, order_status="shipped", order_delivered_customer_date="")
    row = type_orders(df_from([rec])).collect()[0]
    assert row.delivered_late is None and row.delivery_days is None
    assert row.is_delivered is False


def test_on_time_delivery(df_from):
    rec = dict(BASE, order_estimated_delivery_date="2017-01-10 00:00:00")
    assert type_orders(df_from([rec])).collect()[0].delivered_late is False

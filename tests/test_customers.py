from ecom.transformations.customers import build_customer_changes, type_customers


def test_normalization(df_from):
    df = df_from(
        [
            {
                "customer_id": "c1",
                "customer_unique_id": "u1",
                "customer_zip_code_prefix": "1037",
                "customer_city": "sao paulo",
                "customer_state": "sp",
            }
        ]
    )
    row = type_customers(df).collect()[0]
    assert (row.city, row.state, row.zip_code_prefix) == ("Sao Paulo", "SP", "01037")


def test_build_customer_changes(spark, df_from):
    customers = type_customers(
        df_from(
            [
                {
                    "customer_id": "c1",
                    "customer_unique_id": "u1",
                    "customer_zip_code_prefix": "01037",
                    "customer_city": "sao paulo",
                    "customer_state": "SP",
                }
            ]
        )
    )
    orders = spark.createDataFrame(
        [("c1", "2017-01-01 10:00:00")], "customer_id string, ts string"
    ).selectExpr("customer_id", "to_timestamp(ts) AS purchased_at")
    updates = df_from(
        [
            {
                "customer_unique_id": "u1",
                "customer_zip_code_prefix": "20000",
                "customer_city": "rio de janeiro",
                "customer_state": "rj",
                "change_ts": "2017-06-01 00:00:00",
            }
        ]
    )
    rows = build_customer_changes(customers, orders, updates).orderBy("change_ts").collect()
    assert [r.change_source for r in rows] == ["order", "update"]
    assert rows[1].city == "Rio De Janeiro"

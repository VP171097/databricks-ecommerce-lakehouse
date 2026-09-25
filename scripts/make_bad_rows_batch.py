"""Create a batch with deliberately broken rows to prove the quality rules.

Usage: python scripts/make_bad_rows_batch.py --from-batch 2018_08 --out-batch 9999_bad_rows
"""

import argparse
from pathlib import Path

import pandas as pd
from _common import append_manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-batch", default="2018_08")
    ap.add_argument("--out-batch", default="9999_bad_rows")
    ap.add_argument("--batches", default="data/batches", type=Path)
    args = ap.parse_args()

    src = args.batches / args.from_batch

    def read(source: str) -> pd.DataFrame:
        return pd.read_csv(
            src / source / f"{source}_{args.from_batch}.csv", dtype=str, keep_default_na=False
        )

    orders = read("orders").sort_values("order_id").head(20).reset_index(drop=True)
    new_ids = {old: f"bad_{i:02d}" for i, old in enumerate(orders["order_id"])}
    items = read("order_items")
    items = items[items["order_id"].isin(new_ids)].copy().reset_index(drop=True)
    reviews = read("order_reviews")
    reviews = reviews[reviews["order_id"].isin(new_ids)].copy().reset_index(drop=True)

    orders["order_id"] = orders["order_id"].map(new_ids)
    items["order_id"] = items["order_id"].map(new_ids)
    reviews["order_id"] = reviews["order_id"].map(new_ids)
    reviews["review_id"] = "bad_" + reviews["review_id"]

    orders.loc[0:2, "order_id"] = ""  # valid_order_id
    orders.loc[3:5, "order_status"] = "lost"  # valid_status
    orders.loc[6:7, "order_purchase_timestamp"] = "not-a-date"  # valid_purchase_ts
    items.loc[0:2, "price"] = "-10.00"  # positive_price
    items.loc[3:4, "freight_value"] = "-1"  # non_negative_freight
    reviews.loc[0:2, "review_score"] = "7"  # valid_score

    manifest = []
    for source, df in {"orders": orders, "order_items": items, "order_reviews": reviews}.items():
        folder = args.batches / args.out_batch / source
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{source}_{args.out_batch}.csv"
        df.to_csv(path, index=False)
        manifest.append(
            {
                "batch_id": args.out_batch,
                "source": source,
                "path": str(path.relative_to(args.batches)),
                "rows": len(df),
                "bytes": path.stat().st_size,
            }
        )
    append_manifest(args.batches / "batch_manifest.csv", manifest)

    print("Expected quarantined rows:")
    print("  orders        8  (3 blank ids, 3 unknown status, 2 bad timestamps)")
    print(f"  order_items   {min(5, len(items))}  (3 negative prices, 2 negative freight)")
    print(f"  order_reviews {min(3, len(reviews))}  (score 7)")


if __name__ == "__main__":
    main()

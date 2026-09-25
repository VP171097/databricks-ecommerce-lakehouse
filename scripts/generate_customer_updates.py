"""Create "customer moved" records so the SCD Type 2 dimension has real history.

Usage: python scripts/generate_customer_updates.py --batch 2017_06 --pct 2 --seed 42
"""

import argparse
import calendar
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from _common import append_manifest

from ecom.config import SOURCE_FILES


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch", required=True, help="YYYY_MM batch folder to place changes in")
    ap.add_argument("--pct", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--raw", default="data/raw", type=Path)
    ap.add_argument("--out", default="data/batches", type=Path)
    args = ap.parse_args()

    read = lambda s: pd.read_csv(  # noqa: E731
        args.raw / SOURCE_FILES[s], dtype=str, keep_default_na=False
    )
    customers, orders = read("customers"), read("orders")

    first = orders.merge(customers[["customer_id", "customer_unique_id"]], on="customer_id")
    first["month"] = pd.to_datetime(first["order_purchase_timestamp"]).dt.strftime("%Y_%m")
    first_month = first.groupby("customer_unique_id")["month"].min()
    eligible = first_month[first_month < args.batch].index.to_series()
    sample = eligible.sample(frac=args.pct / 100, random_state=args.seed)

    current = customers.drop_duplicates("customer_unique_id", keep="last").set_index(
        "customer_unique_id"
    )
    pool = (
        customers[["customer_zip_code_prefix", "customer_city", "customer_state"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    rng = random.Random(args.seed)
    year, month = (int(x) for x in args.batch.split("_"))
    last_day = calendar.monthrange(year, month)[1]
    rows = []
    for uid in sample:
        current_city = current.at[uid, "customer_city"]
        while True:
            cand = pool.iloc[rng.randrange(len(pool))]
            if cand["customer_city"] != current_city:
                break
        ts = datetime(
            year,
            month,
            rng.randint(1, last_day),
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        rows.append(
            {
                "customer_unique_id": uid,
                "customer_zip_code_prefix": cand["customer_zip_code_prefix"],
                "customer_city": cand["customer_city"],
                "customer_state": cand["customer_state"],
                "change_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    folder = args.out / args.batch / "customer_updates"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"customer_updates_{args.batch}.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    append_manifest(
        args.out / "batch_manifest.csv",
        [
            {
                "batch_id": args.batch,
                "source": "customer_updates",
                "path": str(path.relative_to(args.out)),
                "rows": len(rows),
                "bytes": path.stat().st_size,
            }
        ],
    )
    print(f"{len(rows)} customer updates written to {path}")


if __name__ == "__main__":
    main()

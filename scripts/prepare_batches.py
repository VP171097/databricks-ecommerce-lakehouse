"""Split the Olist files into monthly batch folders that simulate daily exports.

Usage: python scripts/prepare_batches.py --raw data/raw --out data/batches [--months N] [--force]
"""

import argparse
import csv
import shutil
import sys
from pathlib import Path

import pandas as pd
from _common import MANIFEST_FIELDS, ROOT  # noqa: F401  (ROOT puts src on sys.path)

from ecom.config import SOURCE_FILES

REFERENCE = ["products", "sellers", "geolocation", "category_translation"]
ORDER_CHILDREN = ["order_items", "order_payments", "order_reviews"]
CHECKED = ["orders", "order_items", "order_payments", "order_reviews", "customers"]


def read_source(raw: Path, source: str) -> pd.DataFrame:
    return pd.read_csv(raw / SOURCE_FILES[source], dtype=str, keep_default_na=False)


def write(df: pd.DataFrame, out: Path, batch_id: str, source: str, manifest: list) -> None:
    folder = out / batch_id / source
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{source}_{batch_id}.csv"
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
    manifest.append(
        {
            "batch_id": batch_id,
            "source": source,
            "path": str(path.relative_to(out)),
            "rows": len(df),
            "bytes": path.stat().st_size,
        }
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="data/raw", type=Path)
    ap.add_argument("--out", default="data/batches", type=Path)
    ap.add_argument("--months", type=int, help="only the first N months")
    ap.add_argument("--force", action="store_true", help="delete and recreate the output")
    args = ap.parse_args()

    if args.out.exists():
        if not args.force:
            sys.exit(f"{args.out} exists. Use --force to recreate it.")
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True)

    data = {s: read_source(args.raw, s) for s in SOURCE_FILES}
    manifest: list[dict] = []

    for source in REFERENCE:
        write(data[source], args.out, "0000_initial", source, manifest)

    orders = data["orders"]
    ts = pd.to_datetime(
        orders["order_purchase_timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    if ts.isna().any():
        sys.exit(f"{int(ts.isna().sum())} orders have an unparseable purchase timestamp.")
    batch = ts.dt.strftime("%Y_%m")
    months = sorted(batch.unique())
    if args.months:
        months = months[: args.months]

    for month in months:
        o = orders[batch == month]
        order_ids, customer_ids = set(o["order_id"]), set(o["customer_id"])
        write(o, args.out, month, "orders", manifest)
        for child in ORDER_CHILDREN:
            sub = data[child][data[child]["order_id"].isin(order_ids)]
            if len(sub):
                write(sub, args.out, month, child, manifest)
        cust = data["customers"][data["customers"]["customer_id"].isin(customer_ids)]
        if len(cust):
            write(cust, args.out, month, "customers", manifest)
        print(f"{month}: {len(o):>6} orders")

    pd.DataFrame(manifest, columns=MANIFEST_FIELDS).to_csv(
        args.out / "batch_manifest.csv", index=False
    )

    totals = pd.DataFrame(manifest).groupby("source")["rows"].sum()
    print(f"\n{'source':<22}{'original':>10}{'batched':>10}{'diff':>8}")
    failed = False
    for source in SOURCE_FILES:
        original, batched = len(data[source]), int(totals.get(source, 0))
        diff = original - batched
        print(f"{source:<22}{original:>10}{batched:>10}{diff:>8}")
        if source in CHECKED and diff != 0 and not args.months:
            failed = True
    print(f"\n{len(months)} monthly batches written to {args.out}")
    if failed:
        sys.exit("Reconciliation failed: some rows were not assigned to a batch.")


if __name__ == "__main__":
    main()

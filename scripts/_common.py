"""Shared helpers for the local scripts."""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

MANIFEST_FIELDS = ["batch_id", "source", "path", "rows", "bytes"]


def append_manifest(manifest: Path, rows: list[dict]) -> None:
    new_file = not manifest.exists()
    with manifest.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def read_manifest(manifest: Path) -> list[dict]:
    if not manifest.exists():
        sys.exit(f"Manifest not found: {manifest}. Run prepare_batches.py first.")
    with manifest.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))

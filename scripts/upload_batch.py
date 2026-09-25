"""Upload batch files to the landing Volume (default) or to ADLS Gen2 (Azure track).

Usage:
  python scripts/upload_batch.py --batch 0000_initial
  python scripts/upload_batch.py --next | --all [--dry-run]
  python scripts/upload_batch.py --target adls --account <storage> --batch 2016_09
"""

import argparse
import csv
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from _common import read_manifest

from ecom.config import LANDING_PATH, SOURCES

LOG = Path("data/upload_log.csv")
SPECIAL = {"9999_bad_rows"}  # only uploaded when named explicitly


def retry(fn, attempts: int = 3):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if i == attempts - 1:
                raise
            wait = 2 ** (i + 1)
            print(f"   retry in {wait}s after: {exc}")
            time.sleep(wait)


class VolumeTarget:
    def __init__(self, profile: str, base: str):
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient(profile=profile)
        self.base = base

    def remote(self, source: str, name: str) -> str:
        return f"{self.base}/{source}/{name}"

    def exists(self, path: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.w.files.get_metadata(path)
            return True
        except NotFound:
            return False

    def ensure_dir(self, source: str) -> None:
        self.w.files.create_directory(f"{self.base}/{source}")

    def upload(self, local: Path, path: str) -> None:
        self.w.files.create_directory(path.rsplit("/", 1)[0])
        with local.open("rb") as fh:
            self.w.files.upload(path, fh, overwrite=False)


class AdlsTarget:
    def __init__(self, account: str, container: str, prefix: str):
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        svc = DataLakeServiceClient(
            f"https://{account}.dfs.core.windows.net", credential=DefaultAzureCredential()
        )
        self.fs = svc.get_file_system_client(container)
        self.prefix = prefix.strip("/")
        self.account, self.container = account, container

    def remote(self, source: str, name: str) -> str:
        return f"{self.prefix}/{source}/{name}"

    def exists(self, path: str) -> bool:
        return self.fs.get_file_client(path).exists()

    def ensure_dir(self, source: str) -> None:
        directory = self.fs.get_directory_client(f"{self.prefix}/{source}")
        if not directory.exists():
            directory.create_directory()

    def upload(self, local: Path, path: str) -> None:
        directory = self.fs.get_directory_client(path.rsplit("/", 1)[0])
        if not directory.exists():
            directory.create_directory()
        with local.open("rb") as fh:
            self.fs.get_file_client(path).upload_data(fh.read(), overwrite=False)


def uploaded_batches() -> set[str]:
    if not LOG.exists():
        return set()
    with LOG.open(newline="") as fh:
        return {row["batch_id"] for row in csv.DictReader(fh)}


def log(rows: list[dict]) -> None:
    new = not LOG.exists()
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "batch_id",
                "source",
                "file",
                "target",
                "remote_path",
                "bytes",
                "uploaded_at",
            ],
        )
        if new:
            w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch")
    group.add_argument("--next", action="store_true")
    group.add_argument("--all", action="store_true")
    group.add_argument(
        "--init-folders", action="store_true", help="only create one landing folder per source"
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--profile", default="portfolio")
    ap.add_argument("--target", choices=["volume", "adls"], default="volume")
    ap.add_argument("--volume-path", default=LANDING_PATH)
    ap.add_argument("--account", help="storage account (adls target)")
    ap.add_argument("--container", default="landing")
    ap.add_argument("--prefix", default="ecom")
    ap.add_argument("--batches", default="data/batches", type=Path)
    args = ap.parse_args()

    if args.init_folders:
        target = (
            AdlsTarget(args.account, args.container, args.prefix)
            if args.target == "adls"
            else VolumeTarget(args.profile, args.volume_path)
        )
        for source in SOURCES:
            target.ensure_dir(source)
            print(f"folder ready: {source}")
        return

    manifest = read_manifest(args.batches / "batch_manifest.csv")
    all_batches = sorted({r["batch_id"] for r in manifest} - SPECIAL)
    done = uploaded_batches()
    pending = [b for b in all_batches if b not in done]
    if args.batch:
        todo = [args.batch]
    elif args.next:
        todo = pending[:1]
    else:
        todo = pending
    if not todo:
        print("Nothing to upload.")
        return

    if args.dry_run:
        target = None
    elif args.target == "adls":
        if not args.account:
            sys.exit("--account is required for --target adls")
        target = AdlsTarget(args.account, args.container, args.prefix)
    else:
        target = VolumeTarget(args.profile, args.volume_path)
    remote_of = (
        target.remote
        if target
        else lambda source, name: (
            f"{args.prefix if args.target == 'adls' else args.volume_path}/{source}/{name}"
        )
    )

    if target:  # every source folder must exist before the pipeline starts
        for source in SOURCES:
            target.ensure_dir(source)

    uploaded = skipped = failed = 0
    for i, batch_id in enumerate(todo):
        files = [r for r in manifest if r["batch_id"] == batch_id]
        if not files:
            print(f"Unknown batch {batch_id}")
            failed += 1
            continue
        print(f"Batch {batch_id}: {len(files)} files")
        rows = []
        for rec in files:
            local = args.batches / rec["path"]
            remote = remote_of(rec["source"], local.name)
            if args.dry_run:
                print(f"   would upload {local} -> {remote}")
                continue
            try:
                if retry(lambda r=remote: target.exists(r)):
                    print(f"   skipped (immutable): {remote}")
                    skipped += 1
                    continue
                retry(lambda lc=local, r=remote: target.upload(lc, r))
                uploaded += 1
                print(f"   uploaded {remote}")
                rows.append(
                    {
                        "batch_id": batch_id,
                        "source": rec["source"],
                        "file": local.name,
                        "target": args.target,
                        "remote_path": remote,
                        "bytes": local.stat().st_size,
                        "uploaded_at": datetime.now(UTC).isoformat(),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"   FAILED {remote}: {exc}")
        if rows:
            log(rows)
        if args.all and i < len(todo) - 1 and not args.dry_run:
            time.sleep(5)

    print(f"\nuploaded={uploaded} skipped={skipped} failed={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

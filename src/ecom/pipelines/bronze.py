"""Bronze: one incremental Auto Loader streaming table per source (raw strings)."""

import os
import sys

def _setup_sys_path():
    candidates = []
    if "__file__" in globals() and __file__:
        raw = __file__
        candidates.extend([
            raw,
            f"/Workspace/Users/{raw.lstrip('/')}",
            f"/Workspace/{raw.lstrip('/')}",
        ])
    cwd = os.getcwd()
    candidates.extend([cwd, f"/Workspace/{cwd.lstrip('/')}"])

    for candidate in candidates:
        curr = candidate
        for _ in range(7):
            src_dir = os.path.join(curr, "src")
            if os.path.isdir(src_dir) and src_dir not in sys.path:
                sys.path.insert(0, src_dir)
                return
            if os.path.isdir(os.path.join(curr, "ecom")) and curr not in sys.path:
                sys.path.insert(0, curr)
                return
            parent = os.path.dirname(curr)
            if parent == curr:
                break
            curr = parent

_setup_sys_path()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom import config as C
from ecom import pipelines_compat as dp

spark = SparkSession.getActiveSession()
LANDING = spark.conf.get("ecom.landing_path", C.LANDING_PATH)
BATCH_RE = r"_(\d{4}_\d{2}|\d{4}_[a-z_]+)\.csv$"


def make_bronze(source: str) -> None:
    @dp.table(
        name=C.fq("bronze", source),
        comment=f"Raw {source} files from landing, loaded incrementally",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(C.bronze_schema(source))  # explicit: empty folders are fine
        )
        if source == "order_reviews":
            reader = reader.option("multiLine", "true").option("quote", '"').option("escape", '"')
        return (
            reader.load(C.landing(source, LANDING) + "/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_batch_id", F.regexp_extract(F.col("_metadata.file_path"), BATCH_RE, 1))
        )


for _source in C.SOURCES:
    make_bronze(_source)

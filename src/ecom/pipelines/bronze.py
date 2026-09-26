"""Bronze: one incremental Auto Loader streaming table per source (raw strings)."""

import os
import sys

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()


def _add_src_to_path() -> None:
    """Make `import ecom` work. Pipelines run source files like notebook cells (no __file__),
    so the src folder comes from the pipeline setting `ecom.src_path` (fallback: __file__)."""
    candidates = [spark.conf.get("ecom.src_path", "")]
    try:
        candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    except NameError:
        pass
    for path in candidates:
        if path.startswith("/Users/"):
            path = "/Workspace" + path
        if path and os.path.isdir(os.path.join(path, "ecom")):
            path = os.path.abspath(path)
            if path not in sys.path:
                sys.path.insert(0, path)
            return
    raise ModuleNotFoundError(
        "Cannot find the ecom package. Set the pipeline configuration key ecom.src_path "
        "to the workspace path of the repo's src folder."
    )


_add_src_to_path()

from pyspark.sql import functions as F

from ecom import config as C
from ecom import pipelines_compat as dp

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

"""Bronze: one incremental Auto Loader streaming table per source (raw strings)."""

import os
import sys

try:  # make `import ecom` work when the pipeline runs this file from the workspace
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
except NameError:
    pass

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
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
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

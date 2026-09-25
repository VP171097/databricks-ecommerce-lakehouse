import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ecom-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def df_from(spark):
    """Build a DataFrame from a list of dicts with all-string columns (like bronze)."""

    def _make(rows: list[dict]):
        cols = list(rows[0].keys())
        schema = ", ".join(f"`{c}` string" for c in cols)
        return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)

    return _make

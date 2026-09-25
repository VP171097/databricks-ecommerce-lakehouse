"""Generic cleaning helpers. Safe under ANSI mode: bad values become null, never errors."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


def trim_all_strings(df: DataFrame) -> DataFrame:
    """Trim every string column; other columns are untouched; column order is kept."""
    cols = [
        F.trim(F.col(f.name)).alias(f.name) if isinstance(f.dataType, StringType) else F.col(f.name)
        for f in df.schema.fields
    ]
    return df.select(cols)


def empty_to_null(df: DataFrame) -> DataFrame:
    """String columns that are empty after trimming become null."""
    cols = [
        F.when(F.trim(F.col(f.name)) == "", None).otherwise(F.col(f.name)).alias(f.name)
        if isinstance(f.dataType, StringType)
        else F.col(f.name)
        for f in df.schema.fields
    ]
    return df.select(cols)


def title_case(col: str | Column) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.initcap(F.lower(c))


def upper_case(col: str | Column) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.upper(c)


def pad_zip(col: str | Column) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.lpad(c, 5, "0")


def parse_ts(col: str | Column, fmt: str = TS_FORMAT) -> Column:
    """Parse a timestamp string; unparseable values become null (never raise)."""
    c = F.col(col) if isinstance(col, str) else col
    return F.try_to_timestamp(c, F.lit(fmt))


def try_cast(name: str, sql_type: str) -> Column:
    """Cast a column by name; invalid values become null instead of failing."""
    return F.expr(f"try_cast(`{name}` AS {sql_type})")


def tech_cols(df: DataFrame) -> list[str]:
    """Technical columns present in a bronze DataFrame."""
    return [c for c in ("_batch_id", "_ingested_at", "_source_file") if c in df.columns]

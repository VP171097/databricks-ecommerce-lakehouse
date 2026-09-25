from pyspark.sql import functions as F

from ecom.transformations.cleaning import empty_to_null, pad_zip, parse_ts, trim_all_strings


def test_trim_all_strings_leaves_numbers(spark):
    df = spark.createDataFrame([(" a ", 1)], "s string, n int")
    row = trim_all_strings(df).collect()[0]
    assert row.s == "a" and row.n == 1


def test_empty_to_null(spark):
    df = spark.createDataFrame([("",), ("  ",), ("x",)], "s string")
    assert [r.s for r in empty_to_null(df).collect()] == [None, None, "x"]


def test_parse_ts_bad_value_is_null(spark):
    df = spark.createDataFrame([("2017-13-45 99:00:00",), ("2017-01-02 03:04:05",)], "s string")
    out = df.select(parse_ts("s").alias("ts")).collect()
    assert out[0].ts is None and out[1].ts is not None


def test_pad_zip(spark):
    df = spark.createDataFrame([("1037",)], "z string")
    assert df.select(pad_zip(F.col("z")).alias("z")).collect()[0].z == "01037"

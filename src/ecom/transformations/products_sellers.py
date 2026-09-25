"""Silver typing for products, sellers, categories and geolocation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom.transformations.cleaning import (
    empty_to_null,
    pad_zip,
    title_case,
    trim_all_strings,
    try_cast,
    upper_case,
)


def type_categories(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    return df.select(
        F.lower("product_category_name").alias("category_name_pt"),
        F.lower("product_category_name_english").alias("category_name_en"),
    ).dropDuplicates(["category_name_pt"])


def type_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    p = empty_to_null(trim_all_strings(products))
    typed = p.select(
        "product_id",
        F.lower("product_category_name").alias("category_name_pt"),
        try_cast("product_name_lenght", "INT").alias("name_length"),
        try_cast("product_description_lenght", "INT").alias("description_length"),
        try_cast("product_photos_qty", "INT").alias("photos_qty"),
        try_cast("product_weight_g", "DOUBLE").alias("weight_g"),
        try_cast("product_length_cm", "DOUBLE").alias("length_cm"),
        try_cast("product_height_cm", "DOUBLE").alias("height_cm"),
        try_cast("product_width_cm", "DOUBLE").alias("width_cm"),
    )
    joined = typed.join(type_categories(translation), "category_name_pt", "left")
    return joined.select(
        "product_id",
        F.coalesce("category_name_pt", F.lit("uncategorized")).alias("category_name_pt"),
        F.coalesce("category_name_en", "category_name_pt", F.lit("uncategorized")).alias(
            "category_name_en"
        ),
        "name_length",
        "description_length",
        "photos_qty",
        "weight_g",
        "length_cm",
        "height_cm",
        "width_cm",
    )


def type_sellers(df: DataFrame) -> DataFrame:
    df = empty_to_null(trim_all_strings(df))
    return df.select(
        "seller_id",
        pad_zip("seller_zip_code_prefix").alias("zip_code_prefix"),
        title_case("seller_city").alias("city"),
        upper_case("seller_state").alias("state"),
    )


def aggregate_geolocation(df: DataFrame) -> DataFrame:
    """One row per zip prefix; coordinates restricted to Brazil's bounding box."""
    df = empty_to_null(trim_all_strings(df))
    typed = df.select(
        pad_zip("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        try_cast("geolocation_lat", "DOUBLE").alias("lat"),
        try_cast("geolocation_lng", "DOUBLE").alias("lng"),
        title_case("geolocation_city").alias("city"),
        upper_case("geolocation_state").alias("state"),
    ).where("lat BETWEEN -34 AND 6 AND lng BETWEEN -74 AND -34")
    return typed.groupBy("zip_code_prefix").agg(
        F.round(F.avg("lat"), 6).alias("lat"),
        F.round(F.avg("lng"), 6).alias("lng"),
        F.mode("city").alias("city"),
        F.mode("state").alias("state"),
        F.count(F.lit(1)).cast("int").alias("source_rows"),
    )

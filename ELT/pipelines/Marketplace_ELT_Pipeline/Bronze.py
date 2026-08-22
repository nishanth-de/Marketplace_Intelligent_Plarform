from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

from schemas.bronze_schemas import (
    orders_schema,
    order_items_schema,
    customer_schema,
    order_payments_schema,
    order_reviews_schema,
    geolocation_schema,
    products_schema,
    sellers_schema,
    product_category_name_translation_schema,
)


# ============================================================
# Configuration
# ============================================================

BRONZE_LANDING_PATH = (
    "/Volumes/dev_catalog/"
    "bronze_operational/"
    "olist_landing/"
    "batch_01_initial/"
)


# ============================================================
# Bronze Orders
# ============================================================

@dp.materialized_view(name="bronze_orders")
def bronze_orders():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(orders_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_orders_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Order Items
# ============================================================

@dp.materialized_view(name="bronze_order_items")
def bronze_order_items():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(order_items_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_order_items_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Customers
# ============================================================

@dp.materialized_view(name="bronze_customers")
def bronze_customers():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_customers_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Order Payments
# ============================================================

@dp.materialized_view(name="bronze_order_payments")
def bronze_order_payments():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(order_payments_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_order_payments_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Order Reviews
# ============================================================

@dp.materialized_view(name="bronze_order_reviews")
def bronze_order_reviews():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(order_reviews_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_order_reviews_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Geolocation
# ============================================================

@dp.materialized_view(name="bronze_geolocation")
def bronze_geolocation():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(geolocation_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_geolocation_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Products
# ============================================================

@dp.materialized_view(name="bronze_products")
def bronze_products():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(products_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_products_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Sellers
# ============================================================

@dp.materialized_view(name="bronze_sellers")
def bronze_sellers():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(sellers_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "olist_sellers_dataset.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================
# Bronze Product Category Translation
# ============================================================

@dp.materialized_view(
    name="bronze_product_category_translation"
)
def bronze_product_category_translation():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(product_category_name_translation_schema)
        .load(
            f"{BRONZE_LANDING_PATH}"
            "product_category_name_translation.csv"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

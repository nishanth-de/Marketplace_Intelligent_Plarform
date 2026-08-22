from pyspark import pipelines as dp
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, trim, lower, initcap, upper, coalesce


@dp.materialized_view(name="dev_catalog.silver_clean.silver_orders")
def silver_orders():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_orders"
        )
        .withColumn(
            "order_status",
            lower(trim(col("order_status")))
        )
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(
    name="dev_catalog.silver_clean.silver_customers"
)
def silver_customers():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_customers"
        )
        .withColumn(
            "customer_city",
            initcap(trim(col("customer_city")))
        )
        .withColumn(
            "customer_state",
            upper(trim(col("customer_state")))
        )
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "_ingested_at",
            "_source_file"
        )
    )

@dp.materialized_view(
    name="dev_catalog.silver_clean.silver_products"
)
def silver_products():

    products = spark.read.table(
        "dev_catalog.bronze_operational.bronze_products"
    ).alias("p")

    translations = spark.read.table(
        "dev_catalog.bronze_operational.bronze_product_category_translation"
    ).alias("t")

    return (
        products
        .join(
            translations,
            on="product_category_name",
            how="left"
        )
        .withColumn(
            "product_category_name",
            coalesce(
                col("t.product_category_name_english"),
                col("p.product_category_name")
            )
        )
        .select(
            col("p.product_id"),
            col("product_category_name"),
            col("p.product_name_lenght"),
            col("p.product_description_lenght"),
            col("p.product_photos_qty"),
            col("p.product_weight_g"),
            col("p.product_length_cm"),
            col("p.product_height_cm"),
            col("p.product_width_cm"),
            col("p._ingested_at"),
            col("p._source_file")
        )
    )


@dp.materialized_view(name="dev_catalog.silver_clean.silver_order_items")
def silver_order_items():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_order_items"
        )
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "product_id",
            trim(col("product_id"))
        )
        .withColumn(
            "seller_id",
            trim(col("seller_id"))
        )
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.silver_clean.silver_order_payments")
def silver_order_payments():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_order_payments"
        )
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "payment_type",
            lower(trim(col("payment_type")))
        )
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.silver_clean.silver_order_reviews")
def silver_order_reviews():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_order_reviews"
        )
        .withColumn(
            "review_id",
            trim(col("review_id"))
        )
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "review_comment_title",
            trim(col("review_comment_title"))
        )
        .withColumn(
            "review_comment_message",
            trim(col("review_comment_message"))
        )
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "_ingested_at",
            "_source_file"
        )
    )

@dp.materialized_view(name="dev_catalog.silver_clean.silver_sellers")
def silver_sellers():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_sellers"
        )
        .withColumn(
            "seller_id",
            trim(col("seller_id"))
        )
        .withColumn(
            "seller_city",
            initcap(trim(col("seller_city")))
        )
        .withColumn(
            "seller_state",
            upper(trim(col("seller_state")))
        )
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "_ingested_at",
            "_source_file"
        )
    )

@dp.materialized_view(name="dev_catalog.silver_clean.silver_geolocation")
def silver_geolocation():

    return (
        spark.read.table(
            "dev_catalog.bronze_operational.bronze_geolocation"
        )
        .withColumn(
            "geolocation_zip_code_prefix",
            trim(col("geolocation_zip_code_prefix"))
        )
        .withColumn(
            "geolocation_city",
            initcap(trim(col("geolocation_city")))
        )
        .withColumn(
            "geolocation_state",
            upper(trim(col("geolocation_state")))
        )
        .withColumn(
            "geolocation_lat",
            col("geolocation_lat").cast(DoubleType())
        )
        .withColumn(
            "geolocation_lng",
            col("geolocation_lng").cast(DoubleType())
        )
        .select(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
            "_ingested_at",
            "_source_file"
        )
    )
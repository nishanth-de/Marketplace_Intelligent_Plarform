from pyspark import pipelines as dp
from pyspark.sql.functions import col, sha2

#===================================================================
# Dimensions
#===================================================================

@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_customer")
def dim_customer():

    customers = spark.read.table(
        "dev_catalog.silver_clean.silver_customers"
    )

    return (
        customers
        .withColumn(
            "customer_key",
            sha2(
                col("customer_id"),
                256
            )
        )
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_seller")
def dim_seller():

    sellers = spark.read.table(
        "dev_catalog.silver_clean.silver_sellers"
    )

    return (
        sellers
        .withColumn(
            "seller_key",
            sha2(
                col("seller_id"),
                256
            )
        )
        .select(
            "seller_key",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_product")
def dim_product():

    products = spark.read.table(
        "dev_catalog.silver_clean.silver_products"
    )

    return (
        products
        .withColumn(
            "product_key",
            sha2(
                col("product_id"),
                256
            )
        )
        .select(
            "product_key",
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "_ingested_at",
            "_source_file"
        )
    )
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

from schemas.bronze_schemas import orders_schema

@dp.materialized_view(name="bronze_orders")
def bronze_orders():
    volume_path = "/Volumes/dev_catalog/bronze_operational/olist_landing/batch_01_initial/olist_orders_dataset.csv"
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(orders_schema)
        .load( volume_path )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
#======================================================
# DETERMINISTIC DE-DUPLICATION
#======================================================
from pyspark.sql.functions import rank, row_number, col, desc
from pyspark.sql.window import Window

def deduplicate_silver(df, pk_columns, orderBy_columns):

    if isinstance(pk_columns, str):
        pk_columns = [pk_columns]

    if isinstance(orderBy_columns, str):
        orderBy_columns = [orderBy_columns]

    window_spec = (Window.partitionBy(*pk_columns)
                         .orderBy(*[col(column).desc() for column in orderBy_columns])) 

    deduped_df = (df.withColumn("row_number", row_number().over(window_spec))
                    .filter(col("row_number") == 1)
                    .drop("row_number"))

    return deduped_df  

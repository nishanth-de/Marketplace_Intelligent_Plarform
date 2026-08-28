from pyspark import pipelines as dp
from pyspark.sql.functions import (
    sequence,
    explode,
    to_date,
    lit,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    dayofyear,
    weekofyear,
    date_format,
    when
)


@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_date")
def dim_date():

    start_date = "2016-01-01"
    end_date = "2018-12-31"

    dates = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(start_date)),
                    to_date(lit(end_date)),
                    lit(1).cast("interval day")
                )
            ).alias("date")
        )
    )

    return (
        dates
        .withColumn(
            "date_key",
            date_format("date", "yyyyMMdd").cast("int")
        )
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", dayofweek("date"))
        .withColumn("day_name", date_format("date", "EEEE"))
        .withColumn("day_of_year", dayofyear("date"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn(
            "is_weekend",
            when(
                dayofweek("date").isin(1, 7),
                True
            ).otherwise(False)
        )
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "day_of_year",
            "week_of_year",
            "is_weekend"
        )
    )
"""
Reader for sales data coming from the silver layer,
normalized for use in the gold layer.
"""
from pyspark.sql import functions as F


def read_sales_silver(spark, table_name: str):

    df = spark.table(table_name)

    return (
        df
        .select(
            F.col("sku_id").cast("string").alias("sku_id"),
            F.col("order_ts").cast("timestamp").alias("order_ts"),
            F.col("sales").cast("long").alias("sales")
        )
        .filter(F.col("sales") > 0)
    )

"""
Reader for product data coming from the silver layer,
normalized for use in the gold layer.
"""

from pyspark.sql import functions as F


def read_product_silver(spark, table_name: str):
    
    df = spark.table(table_name)

    return (
        df
        .select(
            F.col("sku_id").cast("string").alias("sku_id"),
            F.col("price").cast("double").alias("price")
        )
        .dropDuplicates(["sku_id"])
    )

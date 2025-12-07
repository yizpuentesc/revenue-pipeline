"""
Transformations for building daily product revenue in the gold layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_revenue_daily(
    product_df: DataFrame,
    sales_df: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Build daily revenue per product for a given date range.
    """

    # Filter and aggregate sales by date
    sales_filtered = (
        sales_df
        .withColumn("date_id", F.to_date("order_ts"))
        .filter(
            (F.col("date_id") >= F.lit(start_date)) &
            (F.col("date_id") <= F.lit(end_date))
        )
    )

    sales_agg = (
        sales_filtered
        .groupBy("sku_id", "date_id")
        .agg(F.sum("sales").alias("sales"))
    )

    # Generate a calendar table
    calendar_df = (
        sales_df.sql_ctx.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(start_date)),
                    F.to_date(F.lit(end_date)),
                    F.expr("interval 1 day")
                )
            ).alias("date_id")
        )
    )

    # Remove duplicates
    sku_df = (
        product_df
        .select("sku_id")
        .dropDuplicates(["sku_id"])
    )

    # Cross Join product x calendar
    grid = sku_df.crossJoin(calendar_df)

    # Join with price and aggregated sales
    revenue_df = (
        grid
        .join(product_df.select("sku_id", "price"), on="sku_id", how="left")
        .join(sales_agg, on=["sku_id", "date_id"], how="left")
        .withColumn("sales", F.coalesce(F.col("sales"), F.lit(0)))
        .withColumn("revenue", F.col("price") * F.col("sales"))
    )

    return revenue_df.select("sku_id", "date_id", "price", "sales", "revenue")

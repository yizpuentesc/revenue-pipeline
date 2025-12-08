"""
Writer for revenue data into an iceberg format used by the gold layer.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_revenue_table(spark: SparkSession, table_name: str) -> None:

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            sku_id   string,
            date_id  date,
            price    double,
            sales    long,
            revenue  double
        )
        USING iceberg
        PARTITIONED BY (date_id)
    """)


def write_revenue_to_iceberg(
    revenue_df: DataFrame,
    table_name: str,
    start_date: str,
    end_date: str,
) -> None:

    df_filtered = (
        revenue_df
        .filter(
            (F.col("date_id") >= F.lit(start_date)) &
            (F.col("date_id") <= F.lit(end_date))
        )
    )

    (
        df_filtered
        .writeTo(table_name)
        .overwritePartitions()
    )

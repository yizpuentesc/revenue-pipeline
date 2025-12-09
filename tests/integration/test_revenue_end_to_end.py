from pyspark.sql import SparkSession

from revenue_pipeline.sources.product_reader import read_product_silver
from revenue_pipeline.sources.sales_reader import read_sales_silver
from revenue_pipeline.domain.revenue_transformations import build_revenue_daily
from revenue_pipeline.dq.checks import (
    check_not_null_keys,
    check_non_negative_metrics,
    check_revenue_consistency,
    check_single_day,
)


def _create_spark():
    return (
        SparkSession
        .builder
        .appName("revenue-end-to-end-test")
        .master("local[1]")
        .getOrCreate()
    )


def test_revenue_pipeline_end_to_end_single_day():
    spark = _create_spark()

    try:
        process_date = "2025-01-01"

        product_silver_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/product_sample.csv")
        )

        sales_silver_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/sales_sample.csv")
        )

        product_silver_df.createOrReplaceTempView("silver_product_e2e")
        sales_silver_df.createOrReplaceTempView("silver_sales_e2e")

        product_df = read_product_silver(spark, "silver_product_e2e")
        sales_df = read_sales_silver(spark, "silver_sales_e2e")

        revenue_df = build_revenue_daily(
            product_df=product_df,
            sales_df=sales_df,
            start_date=process_date,
            end_date=process_date,
        )

        assert revenue_df.count() > 0
        assert {"sku_id", "date_id", "price", "sales", "revenue"}.issubset(
            set(revenue_df.columns)
        )

        check_not_null_keys(revenue_df)
        check_non_negative_metrics(revenue_df)
        check_revenue_consistency(revenue_df)
        check_single_day(revenue_df, process_date)

    finally:
        spark.stop()

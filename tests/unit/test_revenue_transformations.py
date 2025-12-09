from pyspark.sql import SparkSession

from revenue_pipeline.domain.revenue_transformations import build_revenue_daily


def _create_spark():
    return (
        SparkSession
        .builder
        .appName("revenue-transformations-test")
        .master("local[1]")
        .getOrCreate()
    )


def test_build_revenue_daily_from_csv():
    spark = _create_spark()

    try:
        # Read Silver sample inputs
        product_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/product_sample.csv")
        )

        sales_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/sales_sample.csv")
        )

        start_date = "2025-01-01"
        end_date = "2025-01-31"

      
        revenue_df = build_revenue_daily(
            product_df=product_df,
            sales_df=sales_df,
            start_date=start_date,
            end_date=end_date,
        )

        # Structural validation
        assert set(revenue_df.columns) == {
            "sku_id", "date_id", "price", "sales", "revenue"
        }

        rows = revenue_df.collect()
        assert len(rows) > 0

        for row in rows:
            assert isinstance(row["sku_id"], str)
            assert row["date_id"] is not None
            assert isinstance(row["price"], float)
            assert isinstance(row["sales"], int)
            assert isinstance(row["revenue"], float)

            # Business rule
            assert row["revenue"] == row["price"] * row["sales"]

    finally:
        spark.stop()

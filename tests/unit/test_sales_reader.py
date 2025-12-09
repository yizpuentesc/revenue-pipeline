from pyspark.sql import SparkSession

from revenue_pipeline.sources.sales_reader import read_sales_silver


def _create_spark():
    return (
        SparkSession
        .builder
        .appName("sales-reader-test")
        .master("local[1]")
        .getOrCreate()
    )


def test_read_sales_silver_from_csv():
    spark = _create_spark()

    try:
        # Read real CSV sample as Silver input
        sales_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/sales_sample.csv")
        )

        # Register as Silver table simulation
        sales_df.createOrReplaceTempView("sales_test")

        # Execute reader
        result_df = read_sales_silver(spark, "sales_test")

        # Expected columns
        assert set(result_df.columns) == {"sku_id", "order_ts", "sales"}

        rows = result_df.collect()
        assert len(rows) > 0

        for row in rows:
            # Types normalized by the reader
            assert isinstance(row["sku_id"], str)
            # order_ts is a timestamp; in PySpark it maps to Python datetime
            assert hasattr(row["order_ts"], "year")
            assert isinstance(row["sales"], int)

            # Filter should have removed non-positive sales
            assert row["sales"] > 0

    finally:
        spark.stop()

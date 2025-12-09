from pyspark.sql import SparkSession

from revenue_pipeline.sources.product_reader import read_product_silver


def _create_spark():
    return (
        SparkSession
        .builder
        .appName("product-reader-test")
        .master("local[1]")
        .getOrCreate()
    )


def test_read_product_silver_from_csv():
    spark = _create_spark()

    try:
        # Read real CSV sample as Silver input
        product_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("tests/sample_data/product_sample.csv")
        )

        # Register as Silver table simulation
        product_df.createOrReplaceTempView("product_test")

        # Execute reader
        result_df = read_product_silver(spark, "product_test")

        # Assertions
        assert set(result_df.columns) == {"sku_id", "price"}

        rows = result_df.orderBy("sku_id").collect()

        # No duplicated SKUs
        sku_ids = [r["sku_id"] for r in rows]
        assert len(sku_ids) == len(set(sku_ids))

        # Price must be numeric
        for row in rows:
            assert isinstance(row["sku_id"], str)
            assert isinstance(row["price"], float)

    finally:
        spark.stop()

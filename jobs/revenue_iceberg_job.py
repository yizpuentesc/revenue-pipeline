"""
Job entrypoint for building and writing daily revenue to the gold layer.
The job processes a single process_date provided by the scheduler.
"""

import sys

from pyspark.sql import SparkSession

from revenue_pipeline.config import load_config
from revenue_pipeline.spark_session import create_spark_session
from revenue_pipeline.sources.product_reader import read_product_silver
from revenue_pipeline.sources.sales_reader import read_sales_silver
from revenue_pipeline.domain.revenue_transformations import build_revenue_daily
from revenue_pipeline.writers.iceberg_writer import (
    create_revenue_table,
    write_revenue_to_iceberg,
)
from revenue_pipeline.logging_utils import get_logger


logger = get_logger(__name__)


def main(config_path: str, process_date: str) -> None:

    job_config = load_config(config_path)

    spark: SparkSession = create_spark_session("revenue-iceberg-job")

    try:
        sales_table = job_config["sources"]["sales"]["table"]
        product_table = job_config["sources"]["product"]["table"]
        revenue_table = job_config["target"]["revenue"]["table"]

        start_date = process_date
        end_date = process_date

        logger.info("Starting revenue job")
        logger.info("Process date: %s", process_date)
        logger.info("Sales table: %s", sales_table)
        logger.info("Product table: %s", product_table)
        logger.info("Revenue table: %s", revenue_table)

        product_df = read_product_silver(spark, product_table)
        sales_df = read_sales_silver(spark, sales_table)

        revenue_df = build_revenue_daily(
            product_df=product_df,
            sales_df=sales_df,
            start_date=start_date,
            end_date=end_date,
        )

        create_revenue_table(spark, revenue_table)

        write_revenue_to_iceberg(
            revenue_df=revenue_df,
            table_name=revenue_table,
            start_date=start_date,
            end_date=end_date,
        )

        logger.info("Revenue job finished successfully for date %s.", process_date)

    finally:
        logger.info("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: spark-submit revenue_iceberg_job.py <config_path> <process_date>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])

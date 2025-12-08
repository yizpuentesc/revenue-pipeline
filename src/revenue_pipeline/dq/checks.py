"""
Data quality checks.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_not_null_keys(df: DataFrame) -> None:
    """Primary keys (sku_id, date_id) must not be null."""

    invalid_count = (
        df
        .filter(F.col("sku_id").isNull() | F.col("date_id").isNull())
        .count()
    )

    if invalid_count > 0:
        raise ValueError(f"{invalid_count} rows with null primary keys.")


def check_non_negative_metrics(df: DataFrame) -> None:
    """Sales and revenue must be non-negative."""

    invalid_count = (
        df
        .filter((F.col("sales") < 0) | (F.col("revenue") < 0))
        .count()
    )

    if invalid_count > 0:
        raise ValueError(f"{invalid_count} rows with negative metrics.")


def check_revenue_consistency(df: DataFrame) -> None:
    """Revenue must match its computed components (price and sales)."""

    invalid_count = (
        df
        .filter(F.col("revenue") != F.col("price") * F.col("sales"))
        .count()
    )

    if invalid_count > 0:
        raise ValueError(f"{invalid_count} rows with inconsistent revenue.")


def check_single_day(df: DataFrame, process_date: str) -> None:
    """Only one process date is allowed per run."""

    distinct_days = df.select("date_id").distinct().count()

    if distinct_days != 1:
        raise ValueError(
            f"Expected 1 date partition for {process_date}, found {distinct_days}."
        )

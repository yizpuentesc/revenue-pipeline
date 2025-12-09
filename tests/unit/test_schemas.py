from revenue_pipeline.schemas.product_schema import PRODUCT_SCHEMA
from revenue_pipeline.schemas.sales_schema import SALES_SCHEMA


def test_product_schema_definition():
    expected = {
        "sku_id": "string",
        "price": "double",
    }

    assert PRODUCT_SCHEMA == expected


def test_sales_schema_definition():
    expected = {
        "sku_id": "string",
        "order_ts": "timestamp",
        "sales": "long",
    }

    assert SALES_SCHEMA == expected

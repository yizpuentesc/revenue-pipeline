"""
Schema definition for sales data used by the Gold layer.
"""

SALES_SCHEMA = {
    "sku_id": "string",
    "order_ts": "timestamp",
    "sales": "long",
}

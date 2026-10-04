"""Portable SQLAlchemy schema for SQLite development and PostgreSQL deployment."""
from sqlalchemy import (MetaData, Table, Column, String, Integer, BigInteger,
                        DateTime, Text, ForeignKey, UniqueConstraint, func)

metadata = MetaData()

customers = Table(
    "dim_customer", metadata,
    Column("customer_id", String(32), primary_key=True),
    Column("customer_name", String(120), nullable=False),
    Column("region", String(60), nullable=False),
)
products = Table(
    "dim_product", metadata,
    Column("product_id", String(32), primary_key=True),
    Column("product_name", String(120), nullable=False),
    Column("category", String(60), nullable=False),
    Column("unit_cost_cents", BigInteger, nullable=False),
)
sales = Table(
    "fact_sales", metadata,
    Column("order_id", String(32), primary_key=True),
    Column("customer_id", String(32), ForeignKey("dim_customer.customer_id"), nullable=False),
    Column("product_id", String(32), ForeignKey("dim_product.product_id"), nullable=False),
    Column("order_date", String(10), nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("unit_price_cents", BigInteger, nullable=False),
    Column("discount_bps", Integer, nullable=False),
    Column("gross_revenue_cents", BigInteger, nullable=False),
    Column("net_revenue_cents", BigInteger, nullable=False),
    Column("total_cost_cents", BigInteger, nullable=False),
    Column("gross_profit_cents", BigInteger, nullable=False),
)
quality_rejects = Table(
    "quality_rejects", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), nullable=False),
    Column("source", String(40), nullable=False),
    Column("row_number", Integer, nullable=False),
    Column("reason", String(200), nullable=False),
    Column("raw_json", Text, nullable=False),
)
runs = Table(
    "etl_runs", metadata,
    Column("run_id", String(36), primary_key=True),
    Column("started_at_utc", String(32), nullable=False),
    Column("source_sha256", String(64), nullable=False),
    Column("customers_loaded", Integer, nullable=False),
    Column("products_loaded", Integer, nullable=False),
    Column("orders_valid", Integer, nullable=False),
    Column("orders_rejected", Integer, nullable=False),
    Column("warehouse_order_count", Integer, nullable=False),
)

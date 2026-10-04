"""Validate, transform, upsert and audit CSV retail data.

The local path uses SQLite, while DATABASE_URL can target PostgreSQL. Monetary
values are converted to integer cents to avoid floating-point accounting errors.
"""
import csv
import hashlib
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .model import customers, metadata, products, quality_rejects, runs, sales

INPUT_COLUMNS = {
    "customers": {"customer_id", "customer_name", "region"},
    "products": {"product_id", "product_name", "category", "unit_cost"},
    "orders": {"order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct"},
}


def read_csv(path: Path, required: set[str]) -> list[tuple[int, dict]]:
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path.name}: required columns {sorted(required)}")
        return [(line, row) for line, row in enumerate(reader, start=2)]


def money_cents(value: str) -> int:
    try:
        amount = Decimal(value)
        if not amount.is_finite() or amount < 0:
            raise ValueError("amount must be finite and nonnegative")
        return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("invalid monetary amount") from exc


def validate_dimensions(customer_rows, product_rows):
    clean_customers, clean_products, rejected = {}, {}, []
    for line, row in customer_rows:
        try:
            ident = row["customer_id"].strip()
            name = row["customer_name"].strip()
            region = row["region"].strip()
            if not ident or not name or not region:
                raise ValueError("blank required customer field")
            if len(ident) > 32 or len(name) > 120 or len(region) > 60:
                raise ValueError("customer field exceeds maximum length")
            if ident in clean_customers:
                raise ValueError("duplicate customer_id")
            clean_customers[ident] = dict(customer_id=ident, customer_name=name, region=region)
        except (ValueError, AttributeError) as exc:
            rejected.append(("customers", line, str(exc), row))
    for line, row in product_rows:
        try:
            ident = row["product_id"].strip()
            name = row["product_name"].strip()
            category = row["category"].strip()
            if not ident or not name or not category:
                raise ValueError("blank required product field")
            if len(ident) > 32 or len(name) > 120 or len(category) > 60:
                raise ValueError("product field exceeds maximum length")
            if ident in clean_products:
                raise ValueError("duplicate product_id")
            clean_products[ident] = dict(product_id=ident, product_name=name,
                                        category=category, unit_cost_cents=money_cents(row["unit_cost"]))
        except (ValueError, AttributeError) as exc:
            rejected.append(("products", line, str(exc), row))
    return clean_customers, clean_products, rejected


def validate_orders(order_rows, customer_map, product_map):
    valid, rejected, seen = [], [], set()
    for line, row in order_rows:
        try:
            ident = row["order_id"].strip()
            cid = row["customer_id"].strip()
            pid = row["product_id"].strip()
            if not ident:
                raise ValueError("blank order_id")
            if len(ident) > 32 or len(cid) > 32 or len(pid) > 32:
                raise ValueError("order or reference field exceeds maximum length")
            if ident in seen:
                raise ValueError("duplicate order_id in source")
            if cid not in customer_map or pid not in product_map:
                raise ValueError("unknown customer_id or product_id")
            parsed_date = date.fromisoformat(row["order_date"].strip())
            if parsed_date.isoformat() != row["order_date"].strip():
                raise ValueError("order_date must use YYYY-MM-DD")
            qty = int(row["quantity"])
            if qty <= 0:
                raise ValueError("quantity must be positive")
            price = money_cents(row["unit_price"])
            if price == 0:
                raise ValueError("unit_price must be positive")
            discount = Decimal(row["discount_pct"])
            if not discount.is_finite() or not 0 <= discount <= 1:
                raise ValueError("discount_pct must be between 0 and 1")
            bps = int((discount * 10000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            gross = qty * price
            discount_cents = int((Decimal(gross) * Decimal(bps) / 10000).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP))
            net = gross - discount_cents
            cost = qty * product_map[pid]["unit_cost_cents"]
            seen.add(ident)
            valid.append(dict(order_id=ident, customer_id=cid, product_id=pid,
                              order_date=row["order_date"].strip(), quantity=qty,
                              unit_price_cents=price, discount_bps=bps,
                              gross_revenue_cents=gross, net_revenue_cents=net,
                              total_cost_cents=cost, gross_profit_cents=net-cost))
        except (ValueError, InvalidOperation, KeyError, TypeError, AttributeError) as exc:
            rejected.append(("orders", line, str(exc), row))
    return valid, rejected


def upsert(connection, table, rows: list[dict], primary_key: str):
    if not rows:
        return
    dialect = connection.dialect.name
    if dialect == "sqlite":
        statement = sqlite_insert(table)
    elif dialect == "postgresql":
        statement = pg_insert(table)
    else:
        raise ValueError("Supported databases: SQLite and PostgreSQL")
    updates = {col.name: getattr(statement.excluded, col.name)
               for col in table.columns if col.name != primary_key}
    connection.execute(statement.on_conflict_do_update(
        index_elements=[table.c[primary_key]], set_=updates), rows)


def engine_for_url(url: str):
    if url.startswith("sqlite:///") and url != "sqlite:///:memory:":
        db_path = url[len("sqlite:///"):]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(url, future=True)
    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def enable_foreign_keys(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    return engine


def source_hash(paths):
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def write_gold_summary(warehouse_rows, output_dir: Path, engine_type: str):
    """Produce a per-region/category gold aggregate using Pandas or PySpark."""
    enriched = [dict(order_id=row["order_id"], region=row["region"],
                     category=row["category"], net_revenue_cents=row["net_revenue_cents"],
                     gross_profit_cents=row["gross_profit_cents"])
                for row in warehouse_rows]
    columns = ["region", "category", "order_count", "net_revenue", "gross_profit"]
    if not enriched:
        pd.DataFrame(columns=columns).to_csv(output_dir / "sales_summary.csv", index=False)
        return
    if engine_type == "pandas":
        grouped = (pd.DataFrame(enriched).groupby(["region", "category"], as_index=False)
                   .agg(order_count=("order_id", "count"),
                        net_revenue_cents=("net_revenue_cents", "sum"),
                        gross_profit_cents=("gross_profit_cents", "sum")))
        aggregated = grouped.to_dict(orient="records")
    elif engine_type == "spark":
        try:
            from pyspark.sql import SparkSession, functions as F
        except ImportError as exc:
            raise RuntimeError("Install the Spark extra: pip install -e '.[spark]'") from exc
        spark = SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]")).appName(
            "EnterpriseRetailETL").getOrCreate()
        try:
            df = spark.createDataFrame(enriched)
            grouped = df.groupBy("region", "category").agg(
                F.count("order_id").alias("order_count"),
                F.sum("net_revenue_cents").alias("net_revenue_cents"),
                F.sum("gross_profit_cents").alias("gross_profit_cents"))
            aggregated = [row.asDict() for row in grouped.collect()]
        finally:
            spark.stop()
    else:
        raise ValueError("engine must be pandas or spark")
    result = pd.DataFrame(aggregated)
    result["net_revenue"] = result["net_revenue_cents"].map(lambda n: f"{n / 100:.2f}")
    result["gross_profit"] = result["gross_profit_cents"].map(lambda n: f"{n / 100:.2f}")
    result[columns].sort_values(["region", "category"]).to_csv(
        output_dir / "sales_summary.csv", index=False)


def run_pipeline(data_dir: Path, output_dir: Path, database_url: str, transform_engine="pandas") -> dict:
    data_dir, output_dir = Path(data_dir), Path(output_dir)
    if transform_engine not in ("pandas", "spark"):
        raise ValueError("transform_engine must be pandas or spark")
    if transform_engine == "spark":
        try:
            import pyspark  # noqa: F401; fail before warehouse mutation if missing
        except ImportError as exc:
            raise RuntimeError("Install the Spark extra: pip install -e '[spark]'") from exc
    paths = {name: data_dir / f"{name}.csv" for name in INPUT_COLUMNS}
    inputs = {name: read_csv(paths[name], cols) for name, cols in INPUT_COLUMNS.items()}
    customer_map, product_map, dim_rejects = validate_dimensions(inputs["customers"], inputs["products"])
    valid_orders, order_rejects = validate_orders(inputs["orders"], customer_map, product_map)
    rejects = dim_rejects + order_rejects
    run_id = str(uuid4())
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    engine = engine_for_url(database_url)
    metadata.create_all(engine)
    with engine.begin() as conn:
        upsert(conn, customers, list(customer_map.values()), "customer_id")
        upsert(conn, products, list(product_map.values()), "product_id")
        upsert(conn, sales, valid_orders, "order_id")
        if rejects:
            conn.execute(quality_rejects.insert(), [
                dict(run_id=run_id, source=source, row_number=line,
                     reason=reason, raw_json=json.dumps(row, sort_keys=True))
                for source, line, reason, row in rejects])
        total_count = conn.scalar(select(func.count()).select_from(sales))
        conn.execute(runs.insert().values(
            run_id=run_id, started_at_utc=started,
            source_sha256=source_hash(list(paths.values())),
            customers_loaded=len(customer_map), products_loaded=len(product_map),
            orders_valid=len(valid_orders), orders_rejected=len(rejects),
            warehouse_order_count=total_count))
        joined = conn.execute(select(
            sales.c.order_id, sales.c.order_date, sales.c.customer_id, sales.c.product_id,
            customers.c.region, products.c.category, sales.c.quantity,
            sales.c.net_revenue_cents, sales.c.gross_profit_cents
        ).select_from(sales.join(customers).join(products)).order_by(sales.c.order_id)).mappings().all()
    output_dir.mkdir(parents=True, exist_ok=True)
    detailed = [{**dict(row), "net_revenue": f"{row['net_revenue_cents'] / 100:.2f}",
                 "gross_profit": f"{row['gross_profit_cents'] / 100:.2f}"}
                for row in joined]
    pd.DataFrame(detailed, columns=["order_id", "order_date", "customer_id", "product_id", "region", "category",
                                    "quantity", "net_revenue_cents", "gross_profit_cents",
                                    "net_revenue", "gross_profit"]).to_csv(output_dir / "curated_sales.csv", index=False)
    write_gold_summary(detailed, output_dir, transform_engine)
    report = dict(run_id=run_id, started_at_utc=started, source_sha256=source_hash(list(paths.values())),
                  customers_loaded=len(customer_map), products_loaded=len(product_map),
                  orders_valid=len(valid_orders), orders_rejected=len(rejects),
                  warehouse_order_count=total_count, transform_engine=transform_engine,
                  database_url_backend=engine.dialect.name)
    (output_dir / "run_summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if rejects:
        with (output_dir / "rejected_rows.csv").open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["source", "row_number", "reason", "raw_json"])
            writer.writerows([(src, line, reason, json.dumps(raw, sort_keys=True)) for src, line, reason, raw in rejects])
    else:
        (output_dir / "rejected_rows.csv").write_text("source,row_number,reason,raw_json\n", encoding="utf-8")
    return report

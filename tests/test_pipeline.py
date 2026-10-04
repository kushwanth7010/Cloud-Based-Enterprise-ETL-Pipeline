"""Executable acceptance tests for the local production-equivalent ETL logic."""
import csv
import json
import shutil
import sqlite3
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select

from enterprise_etl.model import quality_rejects, runs, sales
from enterprise_etl.pipeline import money_cents, run_pipeline

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample"


def execute(tmp_path, source=None):
    output = tmp_path / "output"
    database_url = f"sqlite:///{output / 'retail.db'}"
    report = run_pipeline(source or SAMPLE, output, database_url)
    return report, output, database_url


def test_sample_load_and_rejects(tmp_path):
    report, output, url = execute(tmp_path)
    assert (report["customers_loaded"], report["products_loaded"]) == (12, 6)
    assert (report["orders_valid"], report["orders_rejected"]) == (60, 3)
    assert report["warehouse_order_count"] == 60
    assert report["database_url_backend"] == "sqlite"
    sales_data = pd.read_csv(output / "curated_sales.csv")
    assert len(sales_data) == 60
    assert sales_data["order_id"].is_unique
    summary = pd.read_csv(output / "sales_summary.csv")
    assert summary["order_count"].sum() == 60
    assert (summary["net_revenue"].sum() - sales_data["net_revenue"].sum()) < 0.001
    rejected = pd.read_csv(output / "rejected_rows.csv")
    assert len(rejected) == 3
    assert rejected["reason"].str.contains("quantity must be positive").any()
    assert rejected["reason"].str.contains("unknown customer_id").any()
    assert rejected["reason"].str.contains("duplicate order_id").any()
    assert json.loads((output / "run_summary.json").read_text())["run_id"] == report["run_id"]


def test_rerunning_is_idempotent_and_audited(tmp_path):
    first, output, url = execute(tmp_path)
    second, _, _ = execute(tmp_path)
    assert first["source_sha256"] == second["source_sha256"]
    assert first["run_id"] != second["run_id"]
    assert second["warehouse_order_count"] == 60
    engine = create_engine(url)
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(sales)) == 60
        assert conn.scalar(select(func.count()).select_from(runs)) == 2
        assert conn.scalar(select(func.count()).select_from(quality_rejects)) == 6


def test_exact_currency_and_discount(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "customers.csv").write_text("customer_id,customer_name,region\nC1,Alice,East\n")
    (source / "products.csv").write_text("product_id,product_name,category,unit_cost\nP1,Mouse,Tech,1.10\n")
    (source / "orders.csv").write_text(
        "order_id,customer_id,product_id,order_date,quantity,unit_price,discount_pct\n"
        "O1,C1,P1,2026-01-01,3,2.99,0.10\n")
    report, out, url = execute(tmp_path, source)
    assert report["orders_valid"] == 1
    assert report["orders_rejected"] == 0
    engine = create_engine(url)
    with engine.connect() as conn:
        row = conn.execute(select(sales)).mappings().one()
    assert row["gross_revenue_cents"] == 897
    assert row["net_revenue_cents"] == 807  # round 89.7 cents to 90
    assert row["total_cost_cents"] == 330
    assert row["gross_profit_cents"] == 477
    assert money_cents("1.005") == 101  # HALF_UP, not binary float rounding


def test_invalid_rows_quarantine_and_missing_columns(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(SAMPLE, source)
    with (source / "orders.csv").open("a") as stream:
        stream.write("O9999,C001,P001,2026-04-35,1,9.99,0.00\n")
        stream.write("O9998,C001,P001,2026-08-01,1,9.99,1.50\n")
    report, _, _ = execute(tmp_path, source)
    assert report["orders_rejected"] == 5
    assert report["warehouse_order_count"] == 60
    (source / "orders.csv").write_text("incorrect,columns\nfoo,bar\n")
    with pytest.raises(ValueError, match="required columns"):
        execute(tmp_path, source)


def test_dimensions_invalid_and_foreign_key_reject(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "customers.csv").write_text("customer_id,customer_name,region\nC1,Valid,North\nC1,Duplicate,South\n")
    (source / "products.csv").write_text("product_id,product_name,category,unit_cost\nP1,Thing,Tools,bad\n")
    (source / "orders.csv").write_text(
        "order_id,customer_id,product_id,order_date,quantity,unit_price,discount_pct\n"
        "O1,C1,P1,2026-08-01,1,10.00,0.0\n")
    report, _, _ = execute(tmp_path, source)
    assert report["customers_loaded"] == 1
    assert report["products_loaded"] == 0
    assert report["orders_rejected"] == 3
    assert report["warehouse_order_count"] == 0


def test_existing_order_updates_without_creating_a_duplicate(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(SAMPLE, source)
    report, output, url = execute(tmp_path, source)
    with (source / "orders.csv").open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["unit_price"] = "100.00"
    with (source / "orders.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    new_report, _, _ = execute(tmp_path, source)
    assert new_report["warehouse_order_count"] == 60
    assert report["source_sha256"] != new_report["source_sha256"]
    with create_engine(url).connect() as conn:
        record = conn.execute(select(sales).where(sales.c.order_id == "O0001")).mappings().one()
        assert record["unit_price_cents"] == 10000


def test_invalid_first_occurrence_does_not_block_later_valid_order(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "customers.csv").write_text("customer_id,customer_name,region\nC1,Alice,East\n")
    (source / "products.csv").write_text("product_id,product_name,category,unit_cost\nP1,Mouse,Tech,1.10\n")
    (source / "orders.csv").write_text(
        "order_id,customer_id,product_id,order_date,quantity,unit_price,discount_pct\n"
        "O1,C1,P1,2026-01-01,-1,2.99,0.00\n"
        "O1,C1,P1,2026-01-01,2,2.99,0.00\n")
    report, _, _ = execute(tmp_path, source)
    assert report["orders_valid"] == 1
    assert report["orders_rejected"] == 1


def test_spark_dependency_is_checked_before_database_write(tmp_path):
    import importlib.util
    from enterprise_etl.pipeline import run_pipeline
    if importlib.util.find_spec("pyspark") is not None:
        pytest.skip("Spark is installed; test only applies when optional dependency is absent")
    target = tmp_path / "out"
    with pytest.raises(RuntimeError, match="Install the Spark extra"):
        run_pipeline(SAMPLE, target, f"sqlite:///{target / 'warehouse.db'}", "spark")
    assert not (target / "warehouse.db").exists()


def test_sql_reporting_queries_on_loaded_warehouse(tmp_path):
    from sqlalchemy import text
    _, _, url = execute(tmp_path)
    with create_engine(url).connect() as conn:
        rows = conn.execute(text("""SELECT p.category, COUNT(*) AS n
            FROM fact_sales s JOIN dim_product p ON p.product_id=s.product_id
            GROUP BY p.category""")).all()
    assert sum(row.n for row in rows) == 60
    assert len(rows) == 4


def test_bad_money_and_oversized_ids_are_quarantined(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "customers.csv").write_text("customer_id,customer_name,region\nC1,Alice,East\n")
    (source / "products.csv").write_text("product_id,product_name,category,unit_cost\nP1,Mouse,Tech,1.10\n")
    (source / "orders.csv").write_text(
        "order_id,customer_id,product_id,order_date,quantity,unit_price,discount_pct\n"
        "O1,C1,P1,2026-01-01,1,NaN,0.0\n"
        + f"{'X'*33},C1,P1,2026-01-01,1,2.00,0.0\n"
        + "O3,C1,P1,20260101,1,2.00,0.0\n")
    report, _, _ = execute(tmp_path, source)
    assert report["orders_valid"] == 0
    assert report["orders_rejected"] == 3


def test_s3_adapter_paths_with_mock_client(tmp_path, monkeypatch):
    import boto3
    from enterprise_etl.cloud import download_sources, upload_outputs
    calls = []

    class FakeClient:
        def download_file(self, bucket, key, path):
            calls.append(("download", bucket, key))
            Path(path).write_text("synthetic")

        def upload_file(self, path, bucket, key):
            assert Path(path).is_file()
            calls.append(("upload", bucket, key))

    monkeypatch.setattr(boto3, "client", lambda name, endpoint_url=None: FakeClient())
    data = tmp_path / "raw"
    output = tmp_path / "out"
    output.mkdir()
    for name in ("curated_sales.csv", "sales_summary.csv", "rejected_rows.csv", "run_summary.json"):
        (output / name).write_text("test")
    download_sources("input-bucket", "raw", data)
    upload_outputs("output-bucket", "curated", output)
    assert len(calls) == 7
    assert calls[0] == ("download", "input-bucket", "raw/customers.csv")
    assert calls[-1] == ("upload", "output-bucket", "curated/run_summary.json")

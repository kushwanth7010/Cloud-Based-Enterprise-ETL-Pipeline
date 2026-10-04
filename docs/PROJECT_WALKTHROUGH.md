# Practical project walkthrough

## 1. Business objective

A retail business receives customers, products and orders as separate CSV extracts. The records need basic contracts, referential integrity, profitability calculations, durable storage and reproducible region/category reporting. This project implements those steps using invented example data.

## 2. Download and install

For Python 3.11 or 3.12, create and activate a virtual environment, run `python -m pip install -e '.[dev]'`, then run `python -m enterprise_etl.cli`. By default, the CSVs are read from `data/sample/` and warehouse and reports are written to `artifacts/`. The `README.md` contains Windows PowerShell activation instructions and optional Docker, Spark, Airflow and S3 configurations.

## 3. Follow one order through the pipeline

Read the source order in `data/sample/orders.csv`. It contains a customer ID, product ID, date, quantity, unit price and a discount fraction. The pipeline checks that its customer and product exist in the accompanying dimension extracts and that its date, quantity, price and discount are valid. It converts the unit price into cents and calculates gross revenue, net revenue, total cost and gross profit. The result is upserted into `fact_sales` by order ID, with references to `dim_customer` and `dim_product`.

## 4. Audit and error handling

Three synthetic bad source rows are deliberately included: a negative quantity, an unknown customer ID and a duplicate order ID. These are written to `quality_rejects` and exported to `rejected_rows.csv`. Every run records a unique run ID and a combined SHA-256 of the input files in `etl_runs`. Re-running the same source does not duplicate the fact records but does add another run and rejection-audit record.

## 5. Analyze outputs

`artifacts/curated_sales.csv` is a joined detail extract; `artifacts/sales_summary.csv` contains order count, net revenue and gross profit by region and category. Run the sample queries in `sql/analytics_queries.sql` to analyze category profitability, top customers, monthly regional performance and pipeline history. The demo's revenue and profit have no real-world business meaning because all records are synthetic.

## 6. Test and reproduce

Run `python -m pytest -q`. Unit and acceptance tests cover source validations, exact financial calculations, first-valid duplicate handling, rerun idempotency, changed-order upserts, SQL reporting, optional Spark dependency preflight and mocked S3 paths. The default pipeline can be reproduced without a cloud account.

## 7. Explain the technical tradeoffs in an interview

- **Why cents?** Financial arithmetic must not accumulate floating-point rounding errors.
- **Why upserts?** Re-running the same extract should not duplicate operational facts.
- **Why quarantine?** Bad source data remains visible and explainable without contaminating accepted facts.
- **Why SQL dimensions?** Product and customer attributes can be joined consistently across reports.
- **What is cloud-ready rather than cloud-deployed?** This implementation contains S3 adapters, a PostgreSQL Docker configuration, a Spark aggregation switch and an Airflow DAG, but these optional services require separate live setup and validation.
- **What would change at enterprise scale?** Replace driver-memory batch data processing with partitions and Parquet staging, add orchestration monitoring and alerting, implement bulk loads, govern credentials and access, and test recoverability and concurrent pipelines.

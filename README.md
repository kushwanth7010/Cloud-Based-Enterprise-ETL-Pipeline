# Cloud-Based Enterprise Data Engineering & ETL Pipeline

[![Python ETL tests](https://github.com/kushwanth7010/Cloud-Based-Enterprise-ETL-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/kushwanth7010/Cloud-Based-Enterprise-ETL-Pipeline/actions/workflows/ci.yml)

**An executable portfolio project for data engineering and analytics.** Ingests three synthetic retail CSV sources, validates and quarantines invalid records, performs financial transformations, loads a dimensional SQL warehouse, and publishes analytics-ready reports. It supports **local SQLite**, optional **PostgreSQL via Docker**, optional **PySpark aggregation**, optional **Airflow scheduling**, and optional **AWS S3 import/export**.

> **Deployment transparency:** The standard Python/SQLite/Pandas path is locally tested. PostgreSQL, Docker, PySpark, Airflow and AWS S3 are implemented as optional integration paths but require those services to be configured; this repository does **not** claim that a cloud deployment or a distributed Spark run has been verified. All supplied sample input records are synthetic. This is not a production security/compliance certification.

## Architecture

```text
Synthetic CSVs or S3 raw objects
  customers.csv + products.csv + orders.csv
                   |
                   v
        Python extract and schema checks
                   |
                   v
         Record-level validation ---------> rejected_rows.csv
     (keys, references, money, dates,         quality_rejects SQL table
      quantity, discounts, duplicates)
                   |
                   v
         Data normalization and enrichment
           monetary calculations in cents
                   |
                   v
        SQL warehouse (SQLite/PostgreSQL)
          dim_customer   dim_product
                 fact_sales
                 etl_runs
                   |
                   v
         Curated, joined sales extract
                   |
                   v
    Pandas aggregate OR optional PySpark aggregate
                   |
                   v
    sales_summary.csv + run_summary.json
                   |
                   v
             Optional S3 publish

Optional Apache Airflow DAG schedules this same CLI pipeline.
```

## Quick start (Windows PowerShell, macOS, Linux)

```bash
git clone https://github.com/kushwanth7010/Cloud-Based-Enterprise-ETL-Pipeline.git
cd Cloud-Based-Enterprise-ETL-Pipeline
python -m venv .venv
```

Activate the environment: Windows PowerShell: `.venv\Scripts\Activate.ps1`; macOS/Linux: `source .venv/bin/activate`. Then run:

```bash
python -m pip install -e '.[dev]'
python -m enterprise_etl.cli --data-dir data/sample --output-dir artifacts
python -m pytest -q
```

Alternatively run `enterprise-etl` after installation. No cloud account, database server or external dataset is needed for the default path. The command writes the following files to `artifacts/`:

| Output | Contents |
| --- | --- |
| `warehouse.db` | SQL dimensional warehouse, rejected-row audit and run history |
| `curated_sales.csv` | Joined, cleaned transaction-level facts (with cents and currency columns) |
| `sales_summary.csv` | Revenue, profit and order counts by region and product category |
| `rejected_rows.csv` | Rejected source rows with explicit reasons |
| `run_summary.json` | Run identifier, source checksum, counts and backend |

**Sample acceptance results:** 12 customers, 6 products, 63 source order rows, 60 accepted orders and 3 rejected rows (invalid quantity, unknown customer and duplicate order ID). See `results/` for captured outputs. A second run **updates** existing dimensions and facts instead of duplicating them; it adds a separate audit entry for each run and its rejects.

## Data model and transformations

- `dim_customer(customer_id PK, customer_name, region)`
- `dim_product(product_id PK, product_name, category, unit_cost_cents)`
- `fact_sales(order_id PK, customer_id FK, product_id FK, order_date, quantity, unit_price_cents, discount_bps, gross_revenue_cents, net_revenue_cents, total_cost_cents, gross_profit_cents)`
- `quality_rejects(id PK, run_id, source, row_number, reason, raw_json)`
- `etl_runs(run_id PK, started_at_utc, source_sha256, customers_loaded, products_loaded, orders_valid, orders_rejected, warehouse_order_count)`

The `discount_pct` input is a fraction between 0 and 1 (for example, `0.10` means 10%). Monetary values are converted into integer cents using decimal HALF_UP rounding, avoiding binary floating-point errors. Discounts are quantized to basis points, and discount amounts are rounded to the nearest cent. **Gross revenue = quantity × unit price; net revenue = gross revenue − discount; gross profit = net revenue − quantity × unit cost.** These are sample-business definitions, not a claim about a real company's accounting.

Quality checks cover required CSV columns, blank or duplicate IDs, foreign-key references, valid ISO dates, positive quantity and price, finite nonnegative cost, and discount bounds. Invalid records are quarantined and excluded from the fact table; input-level schema failures stop the run before warehouse writes. Each warehouse load uses a database transaction, and matching order IDs are updated on reprocessing. File exports and S3 publishing occur after the database transaction, so a failed export must be retried; there is no distributed transaction across the SQL database and object store.

The project intentionally uses a **simple batch upsert** strategy; it does not implement change-data capture, streaming, GDPR/PII classification, a complete source snapshot delete policy, or production-grade observability.

## PostgreSQL deployment with Docker (optional)

Install Docker Desktop or Docker Engine and Compose, then:

```bash
docker compose up --build etl
```

Compose starts PostgreSQL with a health check, then runs the same ETL application against the `retail` database. The sample developer-only password in `docker-compose.yml` must be replaced before any nonlocal deployment. PostgreSQL is exposed only on `127.0.0.1:5433`; the processed CSVs appear in the local `artifacts/` folder. To rerun: `docker compose run --rm etl`. To shut down: `docker compose down` (the `postgres_data` volume is kept unless you explicitly remove volumes).

## PySpark engine (optional)

Install a supported Java runtime and the Spark optional dependency:

```bash
python -m pip install -e '.[spark]'
python -m enterprise_etl.cli --engine spark --data-dir data/sample --output-dir artifacts
```

In Spark mode, PySpark performs the **gold summary aggregation**, grouping the validated curated records by region and category. The extraction, validation and SQL upsert stages remain Python/SQLAlchemy and do **not** use distributed Spark. For larger real-world datasets, use an object-store/Parquet staging layer and distributed transformations before warehouse loading instead of collecting records into driver memory.

## Apache Airflow orchestration (optional)

The example DAG at `dags/enterprise_etl_dag.py` schedules the existing CLI at 08:00 daily, with two retries and a five-minute retry delay. In a separately configured Airflow 2 environment, install this package, add `dags/` to the configured DAG folder, and configure `DATABASE_URL`, `ETL_INPUT_DIR` and `ETL_OUTPUT_DIR`. Airflow is **not** installed by the default requirements and the DAG has not been exercised in an Airflow service in this environment. In production, Airflow's metadata DB and worker permissions require their own administration.

## AWS S3 import/export (optional)

Supply AWS credentials via a named profile, IAM role or normal AWS SDK credential provider. Never commit credentials. Create the source and destination buckets beforehand and grant least-privilege access. Example:

```bash
export S3_INPUT_BUCKET=my-existing-input-bucket
export S3_INPUT_PREFIX=raw
export S3_OUTPUT_BUCKET=my-existing-output-bucket
export S3_OUTPUT_PREFIX=curated
python -m enterprise_etl.cli
```

On Windows PowerShell, set environment variables using `$env:S3_INPUT_BUCKET = "..."` (and similarly for the others). Input objects must be `raw/customers.csv`, `raw/products.csv`, and `raw/orders.csv`; output objects include `curated/curated_sales.csv`, `curated/sales_summary.csv`, `curated/rejected_rows.csv` and `curated/run_summary.json`. A custom `S3_ENDPOINT_URL` supports testing against compatible object storage such as MinIO. **No actual AWS bucket or upload is claimed by this repository.**

## SQL analytics and automated checks

Open `sql/analytics_queries.sql` for category profitability, top customers, run history and monthly regional trends. `tests/test_pipeline.py` covers the sample load, source rejection reasons, exact monetary calculations, duplicate-safe reprocessing, audit records, malformed dates, invalid discounts, invalid dimensions and missing columns. GitHub Actions runs the local test suite on Python 3.11 and 3.12 and executes a sample load on every push to `main` and on pull requests.

## Suggested portfolio discussion

Be prepared to discuss data contracts, quarantine decisions, idempotent upserts, primary and foreign keys, integer-cent financial arithmetic, normalized dimensions, aggregation choices, operational failure recovery and the distinction between implemented optional integrations and tested deployments. See `docs/INTERVIEW_GUIDE.md` for question-and-answer prompts.

# Project discussion guide

**What problem did this solve?** It converts three synthetic retail operational extracts into a consistent dimensional SQL warehouse and analytics-ready sales summaries, while making data-quality failures visible rather than silently ignoring them.

**What is the architecture?** Batch CSV/S3 extraction → schema and row validation → quarantine → Decimal-based financial transformation → SQL dimension and fact upserts → Pandas or Spark summary → local files / optional S3 publish. A separate optional Airflow DAG calls the same CLI.

**Why a dimensional warehouse?** Customer and product attributes are normalized in dimension tables; transaction-level measures and foreign keys are in `fact_sales`. The design supports consistent regional, product and customer reports.

**How is the job idempotent?** Customer IDs, product IDs and order IDs are primary keys; dialect-specific PostgreSQL/SQLite ON CONFLICT upserts update them on replay. The source hash documents input identity. Audit runs and rejected rows are deliberately append-only, so their counts increase for each re-execution.

**How do you avoid financial rounding problems?** Read CSV money as `Decimal`, convert to integer cents with ROUND_HALF_UP, express discounts in basis points, and aggregate integer amounts in the warehouse. Only convert to formatted currency at the export boundary.

**What happens to invalid data?** Incorrect quantity, duplicate keys, unknown dimension references, malformed dates and discount values outside [0,1] are recorded in `quality_rejects` and exported to `rejected_rows.csv`. Missing required input columns abort the run before database writes.

**How does PostgreSQL differ from SQLite here?** Both backends share a SQLAlchemy schema and transaction boundaries. The upsert uses the appropriate dialect's `ON CONFLICT` API. PostgreSQL is the more suitable service-backed backend for concurrent real-world jobs; SQLite makes the portfolio demo easy to run locally.

**What is Spark's exact role?** When enabled, PySpark groups the already validated and joined records to calculate regional/category counts, revenue and profit. This demo is not a fully distributed ingestion pipeline: Python still reads the raw CSVs, and the Spark stage receives driver-side data. Explain this limitation clearly.

**What would you change to scale?** Store raw and curated data as versioned Parquet in S3, run distributed schema-validated Spark transformations, partition by date, load via warehouse-native bulk operations, manage late-arriving dimensions, add data contracts and test a genuinely remote deployment.

**How would you deploy safely?** Replace developer credentials, use managed secrets/IAM roles, turn on object-store encryption, restrict networking, set up run alerts, backfills and retention policies, and test the Docker/Airflow/AWS paths in the target environment.

**Which claims are verified locally?** Default Python + Pandas + SQLite pipeline and pytest acceptance suite. PostgreSQL, Airflow, Spark and AWS integrations are provided as separate configurations but not verified live in this environment.

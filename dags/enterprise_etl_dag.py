"""Optional Airflow 2 DAG. Install Airflow separately; not required for local CLI."""
import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="enterprise_retail_etl",
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["portfolio", "data-engineering", "retail"],
    description="Daily audited retail ingestion and analytical aggregation",
)
def enterprise_retail_etl():
    @task
    def run_audited_pipeline():
        command = [
            "python", "-m", "enterprise_etl.cli",
            "--data-dir", os.getenv("ETL_INPUT_DIR", "data/sample"),
            "--output-dir", os.getenv("ETL_OUTPUT_DIR", "artifacts"),
            "--engine", os.getenv("ETL_ENGINE", "pandas"),
        ]
        # Environment also supplies DATABASE_URL and optional S3 settings.
        subprocess.run(command, check=True)

    run_audited_pipeline()


enterprise_retail_etl()

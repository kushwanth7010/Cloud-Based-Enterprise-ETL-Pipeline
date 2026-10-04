"""Command-line entry point for local, Airflow and S3 execution."""
import argparse
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from .cloud import download_sources, upload_outputs
from .pipeline import run_pipeline


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run auditable retail ETL")
    parser.add_argument("--data-dir", type=Path, default=Path("data/sample"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--engine", choices=("pandas", "spark"), default="pandas")
    parser.add_argument("--s3-input-bucket", default=os.getenv("S3_INPUT_BUCKET"))
    parser.add_argument("--s3-input-prefix", default=os.getenv("S3_INPUT_PREFIX", "raw"))
    parser.add_argument("--s3-output-bucket", default=os.getenv("S3_OUTPUT_BUCKET"))
    parser.add_argument("--s3-output-prefix", default=os.getenv("S3_OUTPUT_PREFIX", "curated"))
    parser.add_argument("--s3-endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    args = parser.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.s3_input_bucket:
        download_sources(args.s3_input_bucket, args.s3_input_prefix, args.data_dir, args.s3_endpoint_url)
    database_url = args.database_url or f"sqlite:///{args.output_dir.resolve() / 'warehouse.db'}"
    report = run_pipeline(args.data_dir, args.output_dir, database_url, args.engine)
    if args.s3_output_bucket:
        upload_outputs(args.s3_output_bucket, args.s3_output_prefix,
                       args.output_dir, args.s3_endpoint_url)
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    main()

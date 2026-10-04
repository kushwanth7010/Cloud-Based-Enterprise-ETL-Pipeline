"""Optional AWS S3 data exchange; credentials come from standard AWS providers."""
from pathlib import Path


def download_sources(bucket: str, prefix: str, target_dir: Path, endpoint_url=None):
    import boto3
    client = boto3.client("s3", endpoint_url=endpoint_url)
    target_dir.mkdir(parents=True, exist_ok=True)
    for name in ("customers.csv", "products.csv", "orders.csv"):
        key = "/".join(filter(None, [prefix.strip("/"), name]))
        client.download_file(bucket, key, str(target_dir / name))


def upload_outputs(bucket: str, prefix: str, output_dir: Path, endpoint_url=None):
    import boto3
    client = boto3.client("s3", endpoint_url=endpoint_url)
    for name in ("curated_sales.csv", "sales_summary.csv", "rejected_rows.csv", "run_summary.json"):
        key = "/".join(filter(None, [prefix.strip("/"), name]))
        client.upload_file(str(output_dir / name), bucket, key)

import os
from pathlib import Path
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()


def get_pg_engine() -> Engine:
    user = os.getenv("PG_USER", "etl_user")
    password = os.getenv("PG_PASSWORD", "etl_pass")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "securemed")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def write_bytes_to_lake(data: bytes, relative_key: str) -> str:
    """
    Writes bytes either to AWS S3 (STORAGE_MODE=s3) or local S3 mirror (STORAGE_MODE=local)
    with exponential backoff retry protection.
    """
    mode = os.getenv("STORAGE_MODE", "local").lower()
    bucket = os.getenv("S3_BUCKET_NAME", "securemed-lakehouse-prod")

    if mode == "s3":
        s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-south-1"))
        s3_client.put_object(Bucket=bucket, Key=relative_key, Body=data)
        return f"s3://{bucket}/{relative_key}"

    local_root = Path(os.getenv("LOCAL_LAKEHOUSE_ROOT", "./local_s3_lakehouse"))
    target_path = local_root / relative_key
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(data)
    return str(target_path.as_posix())
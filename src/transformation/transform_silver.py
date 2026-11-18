import os
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from src.utils.db_connector import get_pg_engine, write_bytes_to_lake
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("transformation.transform_silver")


# 1. Pydantic Schema Validation Models
class PatientSilverRecord(BaseModel):
    patient_id: str
    full_name_masked: str = Field(..., description="Masked or hashed patient name for compliance")
    ssn_last4: str
    date_of_birth: str
    gender: str | None = None
    city: str | None = None
    updated_at: str


def mask_pii(name: str) -> str:
    """Masks full names for secure healthcare data governance (e.g. 'Aarav Sharma' -> 'Aarav S.')"""
    parts = name.strip().split()
    if len(parts) > 1:
        return f"{parts[0]} {parts[1][0]}."
    return f"{parts[0]} *."


def run_silver_transformation(entity_name: str, bronze_uri: str) -> str:
    logger.info(f"Starting Silver transformation for {entity_name} from Bronze URI: {bronze_uri}")

    # Read raw JSON payload from local lakehouse or S3
    if bronze_uri.startswith("s3://"):
        import boto3
        # Parse bucket and key from s3 uri
        parts = bronze_uri.replace("s3://", "").split("/", 1)
        bucket, key = parts[0], parts[1]
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_data = obj["Body"].read().decode("utf-8")
        records = json.loads(raw_data)
    else:
        # Local file path read
        raw_data = Path(bronze_uri).read_text(encoding="utf-8")
        records = json.loads(raw_data)

    df = pd.DataFrame(records)
    if df.empty:
        logger.warning(f"No records found in bronze payload for {entity_name}.")
        return ""

    # 2. Apply Transformations & PII Masking
    if entity_name == "patients":
        df["full_name_masked"] = df["full_name"].apply(mask_pii)
        df.drop(columns=["full_name"], inplace=True)

    # Convert timestamps and add partition columns
    now_utc = datetime.now(timezone.utc)
    year_str = now_utc.strftime("%Y")
    month_str = now_utc.strftime("%m")
    day_str = now_utc.strftime("%d")

    df["year"] = year_str
    df["month"] = month_str
    df["day"] = day_str

    # 3. Write to Parquet in Memory & Upload to Silver Layer
    table = pa.Table.from_pandas(df)

    # Write partitioned Parquet output bytes in-memory
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="SNAPPY")
    parquet_bytes = sink.getvalue().to_pybytes()

    relative_key = f"silver/{entity_name}/year={year_str}/month={month_str}/day={day_str}/{entity_name}_data.parquet"
    silver_uri = write_bytes_to_lake(parquet_bytes, relative_key)

    logger.info(f"Silver transformation complete for {entity_name} -> {silver_uri} ({len(df)} rows)")
    return silver_uri


if __name__ == "__main__":
    # Test execution pointing to sample bronze path or run via orchestration
    print("Silver transformer module ready.")
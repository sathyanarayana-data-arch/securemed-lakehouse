import uuid
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import text
from src.utils.db_connector import get_pg_engine, write_bytes_to_lake
from src.utils.logger import get_logger

logger = get_logger("ingestion.extract_postgres")


def run_incremental_extraction(pipeline_name: str, source_table: str, entity_name: str) -> str | None:
    engine = get_pg_engine()
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    with engine.begin() as conn:
        wm_row = conn.execute(
            text("SELECT last_watermark_ts FROM etl_meta.pipeline_watermarks WHERE pipeline_name = :p"),
            {"p": pipeline_name},
        ).fetchone()

        if not wm_row:
            raise ValueError(f"Watermark entry missing for pipeline: {pipeline_name}")

        last_wm = wm_row[0]
        logger.info(f"[{pipeline_name}] Fetching records from {source_table} where updated_at > {last_wm}")

        query = text(f"""
            SELECT *
            FROM {source_table}
            WHERE updated_at > :last_wm
            ORDER BY updated_at ASC
        """)
        df = pd.read_sql(query, conn, params={"last_wm": last_wm})

        row_count = len(df)
        if row_count == 0:
            logger.info(f"[{pipeline_name}] No new records found. Pipeline is up to date.")
            return None

        ingest_date = started_at.strftime("year=%Y/month=%m/day=%d")
        relative_key = f"bronze/{entity_name}/{ingest_date}/batch_{run_id}.json"

        payload_bytes = df.to_json(orient="records", date_format="iso").encode("utf-8")
        target_uri = write_bytes_to_lake(payload_bytes, relative_key)

        max_updated_at = pd.to_datetime(df["updated_at"]).max().to_pydatetime()

        conn.execute(
            text("""
                UPDATE etl_meta.pipeline_watermarks
                SET last_watermark_ts = :new_wm, updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = :p
            """),
            {"new_wm": max_updated_at, "p": pipeline_name},
        )

        conn.execute(
            text("""
                INSERT INTO etl_meta.pipeline_audit_log
                (run_id, pipeline_name, stage, source_row_count, target_row_count, status, target_s3_uri, started_at)
                VALUES (:run_id, :p, 'BRONZE_INGEST', :src_cnt, :tgt_cnt, 'SUCCESS', :uri, :started_at)
            """),
            {
                "run_id": run_id,
                "p": pipeline_name,
                "src_cnt": row_count,
                "tgt_cnt": row_count,
                "uri": target_uri,
                "started_at": started_at,
            },
        )

        logger.info(f"[{pipeline_name}] Extracted {row_count} rows -> {target_uri} | New Watermark: {max_updated_at}")
        return target_uri


if __name__ == "__main__":
    run_incremental_extraction("ingest_patients", "clinical.patients", "patients")
    run_incremental_extraction("ingest_encounters", "clinical.encounters", "encounters")
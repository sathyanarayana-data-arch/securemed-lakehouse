from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.extract_postgres import run_incremental_extraction
from src.transformation.transform_silver import run_silver_transformation
from src.ai_pipeline.chunk_documents import run_document_rag_pipeline

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="securemed_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Healthcare Lakehouse: Bronze Ingestion, Silver Transformation, & RAG Embeddings",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["healthcare", "lakehouse", "rag", "athena"],
) as dag:

    def ingest_task(**kwargs):
        p_uri = run_incremental_extraction("ingest_patients", "clinical.patients", "patients")
        e_uri = run_incremental_extraction("ingest_encounters", "clinical.encounters", "encounters")
        return {"patients_bronze": p_uri, "encounters_bronze": e_uri}

    def transform_task(**kwargs):
        ti = kwargs["ti"]
        bronze_uris = ti.xcom_pull(task_ids="ingest_postgres_sources")
        if bronze_uris and bronze_uris.get("patients_bronze"):
            run_silver_transformation("patients", bronze_uris["patients_bronze"])

    def ai_rag_task(**kwargs):
        sample_file = "data/sample_docs/clinical_note_01.txt"
        if pathlib_exists(sample_file):
            run_document_rag_pipeline(sample_file)

    def pathlib_exists(path_str):
        from pathlib import Path
        return Path(path_str).exists()

    t1 = PythonOperator(
        task_id="ingest_postgres_sources",
        python_callable=ingest_task,
    )

    t2 = PythonOperator(
        task_id="transform_silver_layer",
        python_callable=transform_task,
    )

    t3 = PythonOperator(
        task_id="generate_rag_embeddings",
        python_callable=ai_rag_task,
    )

    t1 >> t2 >> t3
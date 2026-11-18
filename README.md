# SecureMed Lakehouse & RAG-Ready ETL Platform

End-to-end serverless Data & AI pipeline designed for healthcare analytics, incremental data ingestion, PII governance, and vector embedding preparation.

## Architecture Overview
- **Source Layer:** Operational PostgreSQL database + Unstructured clinical PDFs/notes
- **Ingestion & State Management:** Idempotent Python extractors with high-watermark incremental loading & exponential backoff retries
- **Storage Layer (AWS S3 Medallion):**
  - `bronze/`: Raw landing zone
  - `silver/`: PII-masked, schema-validated, date-partitioned Parquet (`year=YYYY/month=MM/day=DD`)
  - `gold/`: Curated analytical aggregations
  - `embeddings/`: Chunked clinical text & vector embeddings for RAG workflows
- **Catalog & Primary Analytics:** AWS Glue Data Catalog & **Amazon Athena**
- **Orchestration:** Apache Airflow

## Repository Structure
```text
├── dags/               # Airflow orchestration workflows
├── data/               # Sample unstructured clinical notes
├── sql/
│   ├── postgres/       # Source DDL, watermark tracking & metadata tables
│   └── athena/         # Partition projection, analytical & reconciliation queries
├── src/
│   ├── ingestion/      # Incremental PostgreSQL -> S3 Bronze extractors
│   ├── transformation/ # PII masking, Pydantic validation & Parquet writers
│   ├── ai_pipeline/    # Clinical document chunking & embedding generator
│   └── utils/          # Retry decorators, structured logging & DB/S3 connectors
└── tests/              # Unit & reconciliation test suite

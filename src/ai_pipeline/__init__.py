import os
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from src.utils.db_connector import write_bytes_to_lake
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("ai_pipeline.chunk_documents")


def chunk_text(text: str, chunk_size: int = 300, overlap: int = 50) -> list[str]:
    """Splits large clinical text into semantic token/character chunks for RAG."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end].strip())
        start += chunk_size - overlap
    return [c for c in chunks if c]


def generate_mock_embedding(text: str, dim: int = 128) -> list[float]:
    """Simulates vector embedding generation (e.g., OpenAI text-embedding-3-small)."""
    import random
    random.seed(len(text))
    return [round(random.uniform(-1.0, 1.0), 6) for _ in range(dim)]


def run_document_rag_pipeline(file_path: str) -> str:
    logger.info(f"Processing unstructured document for RAG ingestion: {file_path}")

    doc_path = Path(file_path)
    if not doc_path.exists():
        raise FileNotFoundError(f"Document not found: {file_path}")

    content = doc_path.read_text(encoding="utf-8")
    chunks = chunk_text(content)

    records = []
    doc_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc)

    for idx, chunk in enumerate(chunks):
        records.append({
            "chunk_id": f"{doc_id}_chunk_{idx}",
            "source_document": doc_path.name,
            "chunk_index": idx,
            "chunk_text": chunk,
            "embedding": generate_mock_embedding(chunk, dim=128),
            "created_at": now_utc.isoformat(),
            "year": now_utc.strftime("%Y"),
            "month": now_utc.strftime("%m"),
            "day": now_utc.strftime("%d")
        })

    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="SNAPPY")
    parquet_bytes = sink.getvalue().to_pybytes()

    year_str = now_utc.strftime("%Y")
    month_str = now_utc.strftime("%m")
    day_str = now_utc.strftime("%d")

    relative_key = f"embeddings/clinical_notes/year={year_str}/month={month_str}/day={day_str}/embeddings_batch.parquet"
    target_uri = write_bytes_to_lake(parquet_bytes, relative_key)

    logger.info(f"Generated {len(chunks)} RAG chunks -> Stored at {target_uri}")
    return target_uri


if __name__ == "__main__":
    sample_file = "data/sample_docs/clinical_note_01.txt"
    run_document_rag_pipeline(sample_file)
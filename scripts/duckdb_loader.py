"""Load cleaned Parquet files from MinIO into DuckDB staging tables."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

import duckdb

from scripts.config import (
    DUCKDB_PATH,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logger = logging.getLogger(__name__)

ENTITIES = ["products", "orders", "order_items", "users"]


def _get_connection() -> duckdb.DuckDBPyConnection:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


def _parquet_path(entity: str, dt: datetime) -> str:
    return f"s3://{MINIO_BUCKET}/cleaned/{entity}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/*.parquet"


def _load_entity(conn: duckdb.DuckDBPyConnection, entity: str, dt: datetime) -> None:
    table = f"stg_{entity}"
    path = _parquet_path(entity, dt)
    extracted_date = dt.strftime("%Y-%m-%d")

    # Create table on first run (empty schema)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} AS
        SELECT * FROM read_parquet('{path}') WHERE 1=0
    """)

    # Idempotent: delete existing data for this date, then insert
    conn.execute(f"DELETE FROM {table} WHERE _extracted_date = '{extracted_date}'")
    conn.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{path}')")

    count = conn.execute(f"SELECT count(*) FROM {table} WHERE _extracted_date = '{extracted_date}'").fetchone()[0]
    logger.info("Loaded %d rows into %s for %s", count, table, extracted_date)


def load_all_to_duckdb(execution_date: Optional[datetime] = None) -> None:
    """Entry point called by Airflow or manual invocation."""
    dt = execution_date or datetime.now()
    logger.info("Starting DuckDB load for %s", dt.date())
    conn = _get_connection()
    try:
        for entity in ENTITIES:
            _load_entity(conn, entity, dt)
    finally:
        conn.close()
    logger.info("DuckDB load complete for %s", dt.date())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_all_to_duckdb()

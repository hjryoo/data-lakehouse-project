"""Extract data from DummyJSON API and upload raw JSON to MinIO."""

from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import datetime
from typing import Optional

import aiohttp
from minio import Minio
from tenacity import retry, stop_after_attempt, wait_exponential

from scripts.config import (
    API_BASE_URL,
    API_PAGE_SIZE,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)

logger = logging.getLogger(__name__)

ENTITIES = ["products", "carts", "users"]


def _get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30))
async def _fetch_page(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


async def _extract_entity(session: aiohttp.ClientSession, entity: str) -> list[dict]:
    """Paginate through an entity endpoint and return all records."""
    all_records: list[dict] = []
    skip = 0
    while True:
        url = f"{API_BASE_URL}/{entity}?limit={API_PAGE_SIZE}&skip={skip}"
        data = await _fetch_page(session, url)
        records = data.get(entity, data.get("products", data.get("carts", data.get("users", []))))
        if not records:
            break
        all_records.extend(records)
        total = data.get("total", 0)
        skip += API_PAGE_SIZE
        if skip >= total:
            break
    return all_records


def _upload_to_minio(client: Minio, entity: str, records: list[dict], dt: datetime) -> str:
    """Upload records as JSON to MinIO and return the object path."""
    path = f"raw/{entity}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{entity}.json"
    payload = json.dumps(records, ensure_ascii=False).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        path,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    logger.info("Uploaded %d records to s3://%s/%s", len(records), MINIO_BUCKET, path)
    return path


async def _extract_all(dt: datetime) -> None:
    client = _get_minio_client()
    _ensure_bucket(client)

    async with aiohttp.ClientSession() as session:
        tasks = {entity: _extract_entity(session, entity) for entity in ENTITIES}
        results = {}
        for entity, coro in tasks.items():
            results[entity] = await coro

    for entity, records in results.items():
        _upload_to_minio(client, entity, records, dt)


def run_extraction(execution_date: Optional[datetime] = None) -> None:
    """Entry point called by Airflow or manual invocation."""
    dt = execution_date or datetime.now()
    logger.info("Starting extraction for %s", dt.date())
    asyncio.run(_extract_all(dt))
    logger.info("Extraction complete for %s", dt.date())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_extraction()

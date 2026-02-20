import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lake-bucket")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "30"))

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).resolve().parent.parent / "data" / "duckdb" / "lakehouse.duckdb"),
)

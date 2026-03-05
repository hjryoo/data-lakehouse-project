# Data Lakehouse Project

**English** | [한국어](README.ko.md)

A local ELT pipeline that ingests DummyJSON API data and processes it through MinIO (data lake) -> Spark transform -> DuckDB load -> dbt modeling/testing.

## Components
- Orchestration: Airflow (`dags/ecommerce_elt_dag.py`)
- Storage: MinIO (`docker-compose.yaml`)
- Compute: PySpark
- Warehouse: DuckDB
- Transform: dbt (`dbt_project/models`)

## Project Structure
```text
.
├── dags/
├── scripts/
├── dbt_project/
│   ├── models/
│   └── dbt_project.yml
├── docker-compose.yaml
└── requirements.txt
```

## Quick Start
1. Set up Python virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Start MinIO/Postgres
```bash
docker compose up -d
```

3. Configure environment variables (`.env`)
```env
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET=lake-bucket
MINIO_SECURE=false
API_BASE_URL=https://dummyjson.com
API_PAGE_SIZE=30
DUCKDB_PATH=./data/duckdb/lakehouse.duckdb
```

4. Run Airflow DAG
- DAG ID: `ecommerce_elt`
- Task flow:
  - `extract_and_load_to_lake`
  - `spark_cleanse_transform`
  - `load_to_duckdb`
  - `dbt_run`
  - `dbt_test`

## Run dbt manually
```bash
cd dbt_project
../venv/bin/dbt run --profiles-dir . --target dev
../venv/bin/dbt test --profiles-dir . --target dev
```

## GitHub Commit Guidance
Do not commit the following:
- `.env`
- `venv/`
- `data/`
- `airflow/`
- `dbt_project/target/`
- `dbt_project/logs/`
- `.idea/`

These rules are already reflected in `.gitignore`.

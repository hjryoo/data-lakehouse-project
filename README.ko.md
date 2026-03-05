# 데이터 레이크하우스 프로젝트

[English](README.en.md) | **한국어**

DummyJSON API 데이터를 수집해 MinIO(Data Lake) -> Spark 변환 -> DuckDB 적재 -> dbt 모델링/테스트까지 수행하는 로컬 ELT 파이프라인입니다.

## 구성 요소
- Orchestration: Airflow (`dags/ecommerce_elt_dag.py`)
- Storage: MinIO (`docker-compose.yaml`)
- Compute: PySpark
- Warehouse: DuckDB
- Transform: dbt (`dbt_project/models`)

## 프로젝트 구조
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

## 빠른 시작
1. Python 가상환경 준비
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. MinIO/Postgres 실행
```bash
docker compose up -d
```

3. 환경 변수 설정 (`.env`)
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

4. Airflow DAG 실행
- DAG ID: `ecommerce_elt`
- 태스크 흐름:
  - `extract_and_load_to_lake`
  - `spark_cleanse_transform`
  - `load_to_duckdb`
  - `dbt_run`
  - `dbt_test`

## dbt 수동 실행
```bash
cd dbt_project
../venv/bin/dbt run --profiles-dir . --target dev
../venv/bin/dbt test --profiles-dir . --target dev
```

## GitHub 업로드 가이드
아래 항목은 커밋하지 않습니다.
- `.env`
- `venv/`
- `data/`
- `airflow/`
- `dbt_project/target/`
- `dbt_project/logs/`
- `.idea/`

`.gitignore`에 위 규칙이 반영되어 있습니다.


## Objective

Build a simple ETL pipeline that ingests raw JSON logs, transforms the data into a clean format, models it into a star schema, and loads it into a relational database (PostgreSQL).

---

## Architecture Overview

```
raw_logs.json
      │
      ▼
  ┌────────┐    ┌───────────┐    ┌──────┐    ┌─────────────────┐
  │ Extract│───▶│ Transform │───▶│ Load │───▶│ Quality Checks  │
  └────────┘    └───────────┘    └──────┘    └─────────────────┘
      ▲               │               │
  JSON file      Pandas DF       PostgreSQL
                                 (star schema)
```

The pipeline is orchestrated by **Apache Airflow** and fully containerised with **Docker Compose**.

---

## Project Structure

```
.
├── dags/
│   └── etl_pipeline.py        # Airflow DAG definition
├── etl/
│   ├── __init__.py
│   ├── extract.py              # Read raw JSON
│   ├── transform.py            # Clean, flatten, build star schema DFs
│   ├── load.py                 # Write to PostgreSQL
│   └── quality_checks.py       # Post-load data quality checks
├── sql/
│   └── schema.sql              # DDL for star schema tables
├── raw_logs.json               # Sample raw data
├── Dockerfile                  # Airflow image with ETL deps
├── docker-compose.yml          # Full stack (Airflow + 2× PostgreSQL)
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

---

## Star Schema Design

### `dim_users`
| Column     | Type        | Description                   |
|------------|-------------|-------------------------------|
| user_key   | SERIAL (PK) | Surrogate key                 |
| user_id    | VARCHAR(50) | Natural key from source       |
| created_at | TIMESTAMP   | Row creation timestamp        |

### `dim_actions`
| Column      | Type          | Description                  |
|-------------|---------------|------------------------------|
| action_key  | SERIAL (PK)   | Surrogate key                |
| action_type | VARCHAR(100)  | Action label (login, etc.)   |
| created_at  | TIMESTAMP     | Row creation timestamp       |

### `fact_user_actions`
| Column          | Type        | Description                        |
|-----------------|-------------|------------------------------------|
| action_id       | SERIAL (PK) | Surrogate key                      |
| user_key        | INTEGER (FK) | → dim_users                       |
| action_key      | INTEGER (FK) | → dim_actions                     |
| event_timestamp | TIMESTAMP    | When the action occurred (UTC)    |
| device          | VARCHAR(50)  | Device type (iOS, Android, Web)   |
| location        | VARCHAR(100) | City name                         |
| loaded_at       | TIMESTAMP    | When the row was inserted         |

---

## Approach

### 1. Extract
- Reads `raw_logs.json` from disk into a Python list of dicts.

### 2. Transform
- **Null filtering** – drops rows with missing/empty `user_id` or `action_type`.
- **Timestamp normalisation** – converts all timestamps to UTC ISO 8601 via `pd.to_datetime`.
- **Metadata flattening** – extracts `device` and `location` from the nested `metadata` object.
- **Deduplication** – removes exact duplicate rows.
- **Star schema modelling** – builds `dim_users`, `dim_actions`, and `fact_user_actions` DataFrames with surrogate keys.

### 3. Load
- Creates tables via DDL in `sql/schema.sql` (idempotent `IF NOT EXISTS`).
- Truncates and reloads all tables (full-refresh strategy) to guarantee idempotency.
- Uses SQLAlchemy + psycopg2 to write Pandas DataFrames to PostgreSQL.

### 4. Data Quality Checks
- No NULL keys in fact table
- All foreign keys are valid (referential integrity)
- No duplicate rows in fact table
- All tables have at least one row

### 5. Orchestration (Airflow)
- A single DAG `myoncare_etl_pipeline` with four sequential tasks:
  `extract → transform → load → quality_checks`
- Scheduled `@daily`, with one retry on failure.

### 6. Infrastructure (Docker)
| Service            | Description                              |
|--------------------|------------------------------------------|
| `postgres`         | Data warehouse (PostgreSQL 16)           |
| `airflow-db`       | Airflow metadata database (PostgreSQL 16)|
| `airflow-init`     | Runs DB migrations & creates admin user  |
| `airflow-webserver`| Airflow UI on port 8080                  |
| `airflow-scheduler`| Triggers DAG runs on schedule            |

---

## Quick Start

```bash
# 1. Build and start everything
docker compose up --build -d

# 2. Open Airflow UI
open http://localhost:8080
#   Login: admin / admin

# 3. Trigger the DAG
#    → In the UI, find "myoncare_etl_pipeline" and click ▶ Trigger DAG

# 4. Verify data in PostgreSQL
docker compose exec postgres psql -U etl_user -d myoncare_dw -c \
  "SELECT * FROM fact_user_actions LIMIT 10;"

# 5. Tear down
docker compose down -v
```

## Requirements

- Docker & Docker Compose v2+
- No local Python install needed (everything runs inside containers)


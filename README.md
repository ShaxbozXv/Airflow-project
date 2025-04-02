# Airflow ETL with Dynamic DAGs

This project uses **Apache Airflow** to define and orchestrate three dynamic DAGs that load datasets into a **PostgreSQL** database. The DAGs run sequentially, and each waits for the previous one to complete before proceeding. All data is appended into a single unified table `sofia_data` inside the `sofia_db` database.

---

## Project Structure

```
.
├── dags/
│   └── dynamic_dag.py           # DAG generator script
├── config/
│   └── datasets_config.json     # Dataset metadata (format, columns, dependencies)
├── data/
│   └── technical_task.zip       # Compressed archive of datasets A, B, C
├── docker-compose.yaml          # Full Airflow + Postgres + Redis stack
└── README.md                    # You are here
```

---

## DAG Execution Order

Each DAG runs daily at **08:00 AM** and depends on the previous DAG:

1. **`dag_a`** → loads `dataset_A.csv`
2. **`dag_b`** → loads `dataset_B.json` after `dag_a` completes
3. **`dag_c`** → loads `dataset_C.xlsx` after `dag_b` completes

All datasets are unzipped and appended into the unified PostgreSQL table:

```
sofia_db.sofia_data
```

---

## Setup Instructions

### 1. Requirements

- Docker & Docker Compose installed
- Port `5435` available on host (for Postgres)

---

### 2. Start Airflow + Postgres

```bash
docker-compose up -d
```

Airflow UI will be available at: [http://localhost:8080](http://localhost:8080)  
Login: `airflow` / `airflow`

---

### 3. Provide Input Data

Make sure this file exists:

```
data/technical_task.zip
```

Containing:

- `dataset_A.csv`
- `dataset_B.json`
- `dataset_C.xlsx`

Airflow will unzip this on run.

---

### 4. Config File Structure

The DAGs are driven by:

```json
[
  {
    "dag_id": "dag_a",
    "file_name": "dataset_A.csv",
    "columns": ["order_id", "operation_date", "operation_time"],
    "format": "csv",
    "wait_for_dag": null
  },
  {
    "dag_id": "dag_b",
    "file_name": "dataset_B.json",
    "columns": ["order_id", "operation_date", "operation_time"],
    "format": "json",
    "wait_for_dag": "dag_a"
  },
  {
    "dag_id": "dag_c",
    "file_name": "dataset_C.xlsx",
    "columns": ["order_id", "operation_date", "operation_time"],
    "format": "xlsx",
    "wait_for_dag": "dag_b"
  }
]
```

---

## ▶Running the DAGs

In Airflow UI:

1. Turn on all three DAGs: `dag_a`, `dag_b`, `dag_c`
2. Trigger each DAG manually with the **same execution date** (e.g. `2025-04-02T13:00:00`)
3. DAGs will run in order and load data to `sofia_db.sofia_data`

Verify results in PostgreSQL:

```sql
SELECT * FROM sofia_data;
```

---

## PostgreSQL Connection Details

### In Airflow UI (Admin > Connections)

- **Conn ID**: `postgres_default`
- **Conn Type**: `Postgres`
- **Host**: `postgres` *(internal Docker hostname)*
- **Port**: `5432`
- **Schema**: `sofia_db`
- **Login**: `airflow`
- **Password**: `airflow`

---

### In DBeaver (Local)

- **Host**: `localhost`
- **Port**: `5435`
- **Database**: `sofia_db`
- **Username**: `airflow`
- **Password**: `airflow`
- **Driver**: PostgreSQL

---

## Submission Format

Provide the following in a `.zip` archive:

```
dags/
config/
data/
docker-compose.yaml
README.md
```

---

## Notes

- Python dependencies (e.g. pandas, openpyxl, psycopg2-binary) are installed via Airflow's environment.
- `ExternalTaskSensor` ensures DAG chaining (A → B → C).
- Ideal for local development and dynamic data pipeline testing.

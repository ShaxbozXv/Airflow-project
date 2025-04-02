from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import json
import os
import zipfile
import psycopg2
import openpyxl

# Path and constants
ZIP_PATH = "/opt/airflow/data/technical_task.zip"
EXTRACT_DIR = "/opt/airflow/data/unzipped"
CONFIG_PATH = "/opt/airflow/config/datasets_config"
TARGET_DB = "sofia_db"
TARGET_TABLE = "sofia_data"

# Unzip dataset archive
def unzip_dataset():
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)

# Load the specified file into the unified table
def load_to_postgres(file_name, columns, format):
    file_path = os.path.join(EXTRACT_DIR, file_name)
    hook = PostgresHook(postgres_conn_id="postgres_default", schema=TARGET_DB)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Ensure target table exists
    col_defs = ', '.join([f"{col} TEXT" for col in columns])
    cur.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({col_defs});")

    # Read dataset based on file format
    if format == "csv":
        df = pd.read_csv(file_path)
    elif format == "json":
        with open(file_path, 'r') as f:
            df = pd.json_normalize(json.load(f))
    elif format == "xlsx":
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported format")

    # Filtering columns
    df = df[columns]
    for _, row in df.iterrows():
        values = tuple(str(row[col]) for col in columns)
        placeholders = ','.join(['%s'] * len(values))
        cur.execute(f"INSERT INTO {TARGET_TABLE} VALUES ({placeholders});", values)

    conn.commit()
    cur.close()
    conn.close()

# Load config
with open(CONFIG_PATH) as f:
    config = json.load(f)

# Dynamically generate DAGs
for dag_cfg in config:
    dag_id = dag_cfg["dag_id"]
    file_name = dag_cfg["file_name"]
    columns = dag_cfg["columns"]
    format = dag_cfg["format"]
    wait_for_dag = dag_cfg.get("wait_for_dag")

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }

    dag = DAG(
        dag_id = dag_id,
        default_args = default_args,
        description = f"Load {file_name} into {TARGET_TABLE}",
        schedule_interval = "0 8 * * *",
        start_date = days_ago(1),
        catchup = False,
        tags = ["etl"],
    )
    
    unzip_task = PythonOperator(
        task_id = "unzip_dataset",
        python_callable = unzip_dataset,
        dag = dag,
        
    )

    if wait_for_dag:
        wait_task = ExternalTaskSensor(
            task_id = f"wait_for_{wait_for_dag}",
            external_dag_id = wait_for_dag,
            external_task_id = "load_data",
            allowed_states = ["success"],
            failed_states = ["failed", "skipped"],
            execution_delta = timedelta(minutes=0),
            timeout = 600,
            poke_interval = 30,
            mode = "poke",
            dag = dag,
        )
    else:
        wait_task = None
    
    load_task = PythonOperator(
        task_id = "load_data",
        python_callable = load_to_postgres,
        op_kwargs = {
            "file_name": file_name,
            "columns": columns,
            "format": format,
        },
        dag=dag,
    )

    if wait_task:
        unzip_task >> wait_task >> load_task
    else:
        unzip_task >> load_task
    
    globals()[dag_id] = dag

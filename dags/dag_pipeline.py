"""DAG diario del pipeline de stock de SuperAhorro.

En WSL, definir ETL_PROJECT_ROOT con la ruta Linux del repositorio cuando
este archivo se copie a $AIRFLOW_HOME/dags. Si se mantiene en repo/dags,
la ruta se detecta automáticamente.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_ROOT = Path(os.environ.get("ETL_PROJECT_ROOT", Path(__file__).resolve().parents[1])).resolve()
os.environ.setdefault("DUCKDB_PATH", str(PROJECT_ROOT / "stock.duckdb"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import run_step  # noqa: E402


with DAG(
    dag_id="pipeline_stock_superahorro",
    description="ETL diario de movimientos de stock desde SQLite a DuckDB",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    default_args={"retries": 1},
    tags=["supermercado", "duckdb", "etl"],
) as dag:
    extraer = PythonOperator(task_id="extraer_raw", python_callable=run_step, op_args=["extract"])
    validar = PythonOperator(task_id="validar_registros", python_callable=run_step, op_args=["validate"])
    limpiar = PythonOperator(task_id="limpiar_rechazados", python_callable=run_step, op_args=["clean"])
    unir = PythonOperator(task_id="construir_tabla_final", python_callable=run_step, op_args=["finalize"])
    consultas = PythonOperator(task_id="consultas_de_negocio", python_callable=run_step, op_args=["queries"])

    extraer >> validar >> limpiar >> unir >> consultas

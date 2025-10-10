# -*- coding: utf-8 -*-
"""
DAG con una sola tarea que ejecuta la función run_dlt_load() definida en dlt_comments_pipeline.py
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dlt_comments_pipeline import run_dlt_load  # importamos la función

with DAG(
    dag_id="dlt_comments_2021_bronze",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    tags=["bronze", "dlt", "minio"],
) as dag:

    load_task = PythonOperator(
        task_id="load_comments_to_bronze",
        python_callable=run_dlt_load,
    )

"""
## Simple ETL Flow

DAG demonstrates a simple ETL Workflow
"""

import os
import time

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Define the basic parameters of the DAG, like schedule and start_date
# Fetch name from name of file excluding extension
DAG_ID = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2024, 8, 8, tz="UTC"),
    schedule="*/1 * * * *",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "etl"},
    tags=["ETL"],
):
    start = EmptyOperator(task_id="START")
    wait_10 = PythonOperator(task_id="wait_10", python_callable=lambda: time.sleep(10))
    end = EmptyOperator(task_id="End")

    start >> wait_10 >> end

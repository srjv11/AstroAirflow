"""
## Simple ETL Flow

DAG demonstrates a simple ETL Workflow
"""

import os

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    sense_wait_10 = ExternalTaskSensor(
        task_id="sense_wait_10",
        external_dag_id="trigger_sense1",
        check_existence=True,
        poll_interval=3,
        deferrable=True,
    )
    end = EmptyOperator(task_id="End")

    start >> sense_wait_10 >> end

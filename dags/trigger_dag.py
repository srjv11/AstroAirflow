"""
## Simple ETL Flow

DAG demonstrates a simple ETL Workflow
"""

import os
import time

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label

# Define the basic parameters of the DAG, like schedule and start_date
# Fetch name from name of file excluding extension
DAG_ID = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2012, 1, 1, tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "etl", "retries": 3},
    tags=["ETL"],
):
    start = EmptyOperator(task_id="START")
    trigger = EmptyOperator(task_id="TriggerWorkflow")
    monitor = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(5))
    branch = BranchPythonOperator(task_id="Branch", python_callable=lambda: "End")
    end = EmptyOperator(task_id="End")
    fail = EmptyOperator(task_id="Fail")

    run_simple_etl = TriggerDagRunOperator(
        task_id="trigger_another_dag",
        trigger_dag_id="simple_etl",
        wait_for_completion=False,
        poke_interval=10,
        deferrable=False,
    )

    start >> trigger >> monitor >> branch >> Label("Success") >> end
    branch >> Label("Fail") >> fail >> run_simple_etl

"""
## Simple ETL Flow

DAG demonstrates a simple ETL Workflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pendulum
import time
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label


# Define the basic parameters of the DAG, like schedule and start_date
with DAG(
    dag_id="simple_etl",
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

    start >> trigger >> monitor >> branch >> Label("Success") >> end
    branch >> Label("Fail") >> fail

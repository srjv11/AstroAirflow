import os
import time

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup


def etl_taskgroup(group_id):
    with TaskGroup(
        group_id=group_id,
        prefix_group_id=True,
    ) as tg1:
        start = EmptyOperator(task_id="START")
        trigger = EmptyOperator(task_id="TriggerWorkflow")
        monitor = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(5))
        branch = BranchPythonOperator(task_id="Branch", python_callable=lambda: f"{group_id}.End")
        end = EmptyOperator(task_id="End", trigger_rule="one_success")
        fail = EmptyOperator(task_id="Fail")

        start >> trigger >> monitor >> branch >> Label("Success") >> end
        branch >> Label("Fail") >> fail >> end
    return tg1


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
    run_simple_etl = TriggerDagRunOperator(
        task_id="trigger_another_dag",
        trigger_dag_id="simple_etl",
        wait_for_completion=True,
        poke_interval=10,
        deferrable=True,
    )
    etl_taskgroup("Job1") >> run_simple_etl >> etl_taskgroup("Job2")

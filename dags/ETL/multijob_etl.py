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


# Define the basic parameters of the DAG, like schedule and start_date
with DAG(
    dag_id="multijob_etl",
    start_date=pendulum.datetime(2012, 1, 1, tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "etl"},
    tags=["ETL"],
):
    EtlJobA1 = etl_taskgroup("EtlJobA1")
    EtlJobA2 = etl_taskgroup("EtlJobA2")
    EtlJobA3 = etl_taskgroup("EtlJobA3")
    EtlJobB1 = etl_taskgroup("EtlJobB1")
    EtlJobB2 = etl_taskgroup("EtlJobB2")
    EtlJobB3 = etl_taskgroup("EtlJobB3")
    EtlJobC1 = etl_taskgroup("EtlJobC1")
    EtlJobC2 = etl_taskgroup("EtlJobC2")
    EtlJobC3 = etl_taskgroup("EtlJobC3")

    (
        EtlJobA1
        >> EtlJobA2
        >> EtlJobA3
        >> [
            EtlJobB1,
            EtlJobB2,
            EtlJobB3,
        ]
    )

    EtlJobA2 >> EtlJobC1 >> EtlJobC2 >> EtlJobC3

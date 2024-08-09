import time

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DATASET_SF_BRONZE = Dataset("snowflake://my_db.BRONZE.my_table")
DATASET_SF_SILVER = Dataset("snowflake://my_db.SILVER.my_table")
DATASET_SF_SILVER1 = Dataset("snowflake://my_db.SILVER.my_table2")
DATASET_SF_GOLD = Dataset("snowflake://my_db.GOLD.my_table")
DATASET_SF_BI = Dataset("snowflake://my_db.BI.my_table")
time_asleep = 5

with DAG(
    dag_id="IngestionToBronze",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    tags=["ELTBronze", "MedallionArch"],
):
    t2 = EmptyOperator(
        task_id="SimpleHttpOperator",
        outlets=[DATASET_SF_BRONZE],
    )
    t1 = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(time_asleep))
    t1 >> t2

with DAG(
    dag_id="BronzeToSilver1",
    schedule=[DATASET_SF_BRONZE],
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    tags=["ELTSilver", "MedallionArch"],
):
    t2 = EmptyOperator(
        task_id="SQLExecuteQueryOperator",
        # conn_id="snowflake_default",
        # sql="""
        #   SELECT *
        #   FROM my_db.my_schema.my_table
        #   WHERE "updated_at" >= '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start }}'
        #   AND "updated_at" < '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_end }}';
        # """,
        outlets=[DATASET_SF_SILVER],
    )
    t1 = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(time_asleep))
    t1 >> t2

with DAG(
    dag_id="BronzeToSilver2",
    schedule=[DATASET_SF_BRONZE],
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    tags=["ELTSilver", "MedallionArch"],
):
    t2 = EmptyOperator(
        task_id="SQLExecuteQueryOperator",
        outlets=[DATASET_SF_SILVER1],
    )
    t1 = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(time_asleep))
    t1 >> t2


with DAG(
    dag_id="SilverToGold",
    # schedule=[DATASET_SF_SILVER, DATASET_SF_SILVER1],
    schedule=(DATASET_SF_SILVER & DATASET_SF_SILVER1),
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    tags=["ELTGold", "MedallionArch"],
):
    t2 = EmptyOperator(
        task_id="SQLExecuteQueryOperator",
        outlets=[DATASET_SF_GOLD],
    )
    t1 = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(time_asleep))
    t1 >> t2


with DAG(
    dag_id="BIRefresh",
    schedule=[DATASET_SF_GOLD],
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    tags=["ELTBI", "MedallionArch"],
):
    t2 = EmptyOperator(
        task_id="TableauOperator",
    )
    t1 = PythonOperator(task_id="Monitor", python_callable=lambda: time.sleep(time_asleep))
    t1 >> t2

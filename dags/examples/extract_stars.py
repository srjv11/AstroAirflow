"""Fetch Github Stars for Airflow as of DAG Run."""

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator


def _print_stargazers(github_stats: str, date: str):
    github_stats_json = json.loads(github_stats)
    airflow_stars = github_stats_json.get("stargazers_count")
    print(f"As of {date}, Apache Airflow has {airflow_stars} stars on Github!")


with DAG(
    "extract_stars",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    doc_md=__doc__,
) as dag:
    get_date = BashOperator(task_id="get_date", bash_command="echo {{ ds.format('yyyy') }}")
    query_github_stats = SimpleHttpOperator(
        task_id="query_github_stats",
        endpoint="{{ var.value.endpoint }}",
        method="GET",
        http_conn_id="github_api",
        log_response=True,
    )
    print_stargazers = PythonOperator(
        task_id="print_stars",
        python_callable=_print_stargazers,
        op_args=[
            "{{ ti.xcom_pull(task_ids='query_github_stats') }}",
            "{{ ti.xcom_pull(task_ids='get_date') }}",
        ],
    )
get_date >> query_github_stats >> print_stargazers

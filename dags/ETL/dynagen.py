import json
import os
from glob import glob
import pendulum
from airflow.decorators import dag
from typing import List, Dict, Any
from airflow.operators.empty import EmptyOperator

JSON_CONFIG_DIR: str = os.path.join(os.getcwd(), "include", "configs", "*.json")


def load_config_file(dag_config_path: str) -> Dict[str, Any]:
    """Reads DAG configuration files.

    :param file_name: name of config file.
    :return: Python Dict.
    """
    with open(dag_config_path, "r", encoding="utf-8") as file:
        return json.loads(file.read())


def get_config_jsons(config_dir):
    list_of_jsons: List[str] = glob(config_dir)
    return [load_config_file(dag_config_path) for dag_config_path in list_of_jsons]


for config in get_config_jsons(JSON_CONFIG_DIR):
    dag_id = config["dag_id"]
    start_date = pendulum.datetime(
        year=config["start_date"]["year"],
        month=config["start_date"]["month"],
        day=config["start_date"]["day"],
        tz=config["start_date"]["tz"],
    )
    tags = config["tags"]

    @dag(dag_id=dag_id, start_date=start_date, tags=tags)
    def dynamic_generated_dag():
        start = EmptyOperator(task_id="start")

        for taskelement in config["tasks"]:
            if taskelement["type"] == "EmptyOperator":
                sensor = EmptyOperator(task_id=taskelement["task_id"])

                start >> sensor

    dynamic_generated_dag()

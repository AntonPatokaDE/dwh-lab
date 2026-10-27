from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from common.docker_config import DBT_COMMON


with DAG(
    dag_id="dm_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "dm", "dbt"],
) as dag:

    # Строим только модели слоя DM.
    run_dm = DockerOperator(
        task_id="run_dm",
        command=[
            "dbt",
            "run",
            "--select",
            "path:models/dm",
        ],
        **DBT_COMMON,
    )

    # После построения DM запускаем тесты этого слоя.
    test_dm = DockerOperator(
        task_id="test_dm",
        command=[
            "dbt",
            "test",
            "--select",
            "path:models/dm",
        ],
        **DBT_COMMON,
    )

    run_dm >> test_dm
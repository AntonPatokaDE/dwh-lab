from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

from common.docker_config import DBT_COMMON


with DAG(
    dag_id="dds_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "dds", "dbt"],
) as dag:

    # Строим только модели слоя DDS.
    run_dds = DockerOperator(
        task_id="run_dds",
        command=[
            "dbt",
            "run",
            "--select",
            "path:models/dds",
        ],
        **DBT_COMMON,
    )

    # После построения DDS запускаем тесты этого слоя.
    test_dds = DockerOperator(
        task_id="test_dds",
        command=[
            "dbt",
            "test",
            "--select",
            "path:models/dds",
        ],
        **DBT_COMMON,
    )

    # Если DDS успешно построен и протестирован,
    # запускаем следующий слой — DM.
    trigger_dm = TriggerDagRunOperator(
        task_id="trigger_dm",
        trigger_dag_id="dm_dag",
    )

    run_dds >> test_dds >> trigger_dm
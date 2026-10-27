from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

from common.docker_config import DBT_COMMON


with DAG(
    dag_id="ods_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "ods", "dbt"],
) as dag:

    # Строим только модели слоя ODS.
    run_ods = DockerOperator(
        task_id="run_ods",
        command=[
            "dbt",
            "run",
            "--select",
            "path:models/ods",
        ],
        **DBT_COMMON,
    )

    # После построения ODS запускаем тесты только для этого слоя.
    test_ods = DockerOperator(
        task_id="test_ods",
        command=[
            "dbt",
            "test",
            "--select",
            "path:models/ods",
        ],
        **DBT_COMMON,
    )

    # Если модели построились и тесты прошли,
    # запускаем следующий слой DWH — DDS.
    trigger_dds = TriggerDagRunOperator(
        task_id="trigger_dds",
        trigger_dag_id="dds_dag",
    )

    run_ods >> test_ods >> trigger_dds
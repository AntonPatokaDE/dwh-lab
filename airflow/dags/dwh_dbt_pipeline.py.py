import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DBT_COMMON = {
    "image": "dwh-dbt:1.0",
    "environment": {
    "DBT_PROFILES_DIR": "/usr/app/dbt",
    "GP_HOST": os.environ["GP_HOST"],
    "GP_PORT": os.environ["GP_PORT"],
    "GP_DB": os.environ["GP_DB"],
    "GP_USER": os.environ["GP_USER"],
    "GP_PASSWORD": os.environ["GP_PASSWORD"],
    },
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "dwh-net",
    "working_dir": "/usr/app/dbt",
    "mounts": [
        Mount(
            source="D:/Develop/DEPRACK/dwh-lab/dbt",
            target="/usr/app/dbt",
            type="bind",
        )
    ],
    "mount_tmp_dir": False,
    "auto_remove": "success",
}


with DAG(
    dag_id="dwh_dbt_pipeline",
    schedule=None,
    catchup=False,
    tags=["dwh", "dbt"],
) as dag:

    dbt_run_ods = DockerOperator(
        task_id="run_ods",
        command="dbt run --select path:models/ods",
        **DBT_COMMON,
    )

    dbt_test_ods = DockerOperator(
        task_id="test_ods",
        command="dbt test --select path:models/ods",
        **DBT_COMMON,
    )

    dbt_run_dds = DockerOperator(
        task_id="run_dds",
        command="dbt run --select path:models/dds",
        **DBT_COMMON,
    )

    dbt_test_dds = DockerOperator(
        task_id="test_dds",
        command="dbt test --select path:models/dds",
        **DBT_COMMON,
    )

    dbt_run_dm = DockerOperator(
        task_id="run_dm",
        command="dbt run --select path:models/dm",
        **DBT_COMMON,
    )

    dbt_test_dm = DockerOperator(
        task_id="test_dm",
        command="dbt test --select path:models/dm",
        **DBT_COMMON,
    )

    (
        dbt_run_ods
        >> dbt_test_ods
        >> dbt_run_dds
        >> dbt_test_dds
        >> dbt_run_dm
        >> dbt_test_dm
    )
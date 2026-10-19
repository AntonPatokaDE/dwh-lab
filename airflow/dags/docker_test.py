from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="docker_test",
    schedule=None,
    catchup=False,
) as dag:

    test_docker = DockerOperator(
        task_id="test_docker",
        image="alpine:3.20",
        command="echo HELLO FROM DOCKEROPERATOR",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )
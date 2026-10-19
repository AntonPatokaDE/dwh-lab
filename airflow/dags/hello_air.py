from airflow import DAG

from airflow.operators.bash import BashOperator 

with DAG(
    dag_id = "hello_air",
    schedule = None,
    catchup = False
) as dag:

    start = BashOperator(
        task_id = "start",
        bash_command = "echo START"
    )
    
    hello = BashOperator(
        task_id = "hello",
        bash_command = "echo HELLO AIRFLOW!"
    )
    
    done = BashOperator(
        task_id = "done",
        bash_command = "echo DONE"
    )

    start >> hello >> done
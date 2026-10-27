import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.docker_config import DBT_COMMON, SPARK_COMMON


logger = logging.getLogger(__name__)


# Проверяем, что Airflow может подключиться и к источнику,
# и к gp. Если одно из соединений недоступно, даг сразу падает.
def check_source_and_gp():
    source_hook = PostgresHook(
        postgres_conn_id="source_postgres"
    )

    gp_hook = PostgresHook(
        postgres_conn_id="gp_dwh"
    )

    try:
        source_hook.get_first("SELECT 1")
    except Exception:
        logger.exception(
            "Не удалось подключиться к source PostgreSQL"
        )
        raise

    try:
        gp_hook.get_first("SELECT 1")
    except Exception:
        logger.exception(
            "Не удалось подключиться к Greenplum"
        )
        raise

    logger.info(
        "Подключение к source PostgreSQL и Greenplum успешно"
    )


# Проверяем, что нужная таблица действительно существует в gp
def check_table_exists(gp_hook, table_name):
    schema_name, table = table_name.split(".")

    result = gp_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND table_name = '{table}'
        """
    )

    if result[0] == 0:
        raise RuntimeError(
            f"Таблица {table_name} не существует"
        )


# После загрузки таблица не должна остаться пустой
def check_not_empty(gp_hook, table_name):
    row_count = gp_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        """
    )[0]

    if row_count == 0:
        raise RuntimeError(
            f"Таблица {table_name} пустая"
        )


# Проверяем, что ключевое поле не содержит null
def check_nulls(gp_hook, table_name, key_column):
    null_count = gp_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {key_column} IS NULL
        """
    )[0]

    if null_count > 0:
        raise RuntimeError(
            f"{table_name}: найдено NULL в поле {key_column}"
        )


# Проверяем отсутствие дублей по ключу таблицы
def check_duplicates(gp_hook, table_name, key_column):
    duplicate_count = gp_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {key_column}
            FROM {table_name}
            GROUP BY {key_column}
            HAVING COUNT(*) > 1
        ) t
        """
    )[0]

    if duplicate_count > 0:
        raise RuntimeError(
            f"{table_name}: найдены дубли по полю {key_column}"
        )


# Для full load количество строк в source и stage должно совпадать
def check_source_stage_count(
    source_hook,
    gp_hook,
    source_table,
    stage_table,
):
    source_count = source_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM {source_table}
        """
    )[0]

    stage_count = gp_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM {stage_table}
        """
    )[0]

    if source_count != stage_count:
        raise RuntimeError(
            f"Количество строк не совпадает: "
            f"{source_table}={source_count}, "
            f"{stage_table}={stage_count}"
        )


# Общая проверка stage после завершения обеих Spark-загрузок
# Для каждой таблицы проверяем существование, наличие данных,
# NULL и дубли по ключу, а затем сравниваем число строк с source
def check_stage():
    source_hook = PostgresHook(
        postgres_conn_id="source_postgres"
    )

    gp_hook = PostgresHook(
        postgres_conn_id="gp_dwh"
    )

    tables = [
        {
            "source": "production.product",
            "stage": "stage.product",
            "key": "productid",
        },
        {
            "source": "production.productsubcategory",
            "stage": "stage.productsubcategory",
            "key": "productsubcategoryid",
        },
        {
            "source": "production.productcategory",
            "stage": "stage.productcategory",
            "key": "productcategoryid",
        },
        {
            "source": "sales.salesorderheader",
            "stage": "stage.salesorderheader",
            "key": "salesorderid",
        },
        {
            "source": "sales.salesorderdetail",
            "stage": "stage.salesorderdetail",
            "key": "salesorderdetailid",
        },
    ]

    for table in tables:
        check_table_exists(
            gp_hook,
            table["stage"],
        )

        check_not_empty(
            gp_hook,
            table["stage"],
        )

        check_nulls(
            gp_hook,
            table["stage"],
            table["key"],
        )

        check_duplicates(
            gp_hook,
            table["stage"],
            table["key"],
        )

        check_source_stage_count(
            source_hook,
            gp_hook,
            table["source"],
            table["stage"],
        )

    logger.info(
        "Все проверки STAGE успешно пройдены"
    )


# Эти dbt-макросы создают физические таблицы satge
# В даг для каждого имени ниже будет создана отдельная task
stage_macros = [
    "create_stage_product",
    "create_stage_productsubcategory",
    "create_stage_productcategory",
    "create_stage_salesorderheader",
    "create_stage_salesorderdetail",
]


with DAG(
    dag_id="stage_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "stage", "spark", "dbt"],
) as dag:

    # Сначала убеждаемся, что обе базы доступны
    check_connections = PythonOperator(
        task_id="check_source_and_dwh",
        python_callable=check_source_and_gp,
    )

    # Создаём schema stage, если её ещё нет
    create_stage_schema = DockerOperator(
        task_id="create_stage_schema",
        command=[
            "dbt",
            "run-operation",
            "create_stage_schema",
        ],
        **DBT_COMMON,
    )

    # Создаём отдельную task для каждой stage-таблицы
    stage_tasks = []

    for macro_name in stage_macros:
        task = DockerOperator(
            task_id=macro_name,
            command=[
                "dbt",
                "run-operation",
                macro_name,
            ],
            **DBT_COMMON,
        )

        stage_tasks.append(task)

    # Полная batch-загрузка sales-таблиц из източника в stage
    load_sales = DockerOperator(
        task_id="load_sales_to_stage",
        command=[
            "/opt/spark/app/jobs/full_sales_to_stage.py",
        ],
        **SPARK_COMMON,
    )

    # Полная batch-загрузка product-таблиц из источника в stage
    load_product = DockerOperator(
        task_id="load_product_to_stage",
        command=[
            "/opt/spark/app/jobs/full_product_to_stage.py",
        ],
        **SPARK_COMMON,
    )

    # Проверяем stage только после успешного завершения обеих загрузок
    stage_test = PythonOperator(
        task_id="stage_test",
        python_callable=check_stage,
    )

    # Если stage прошёл проверки, тригерим даг ODS
    trigger_ods = TriggerDagRunOperator(
        task_id="trigger_ods",
        trigger_dag_id="ods_dag",
    )

    check_connections >> create_stage_schema

    create_stage_schema >> stage_tasks

    for stage_task in stage_tasks:
        stage_task >> [load_sales, load_product]

    [load_sales, load_product] >> stage_test >> trigger_ods

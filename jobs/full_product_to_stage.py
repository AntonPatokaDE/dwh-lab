import os
import uuid
import time
import csv

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


# ============================================================
# SETTINGS
# ============================================================

SRC_URL = os.environ["SRC_URL"]
SRC_USER = os.environ["SRC_USER"]
SRC_PASSWORD = os.environ["SRC_PASSWORD"]

GP_URL = os.environ["GP_URL"]
GP_USER = os.environ["GP_USER"]
GP_PASSWORD = os.environ["GP_PASSWORD"]

SOURCE_SYSTEM = "adventureworks"

READ_PARTITIONS = int(
    os.environ.get("READ_PARTITIONS", "1")
)

WRITE_PARTITIONS = int(
    os.environ.get("WRITE_PARTITIONS", "1")
)

BENCHMARK_FILE = (
    "/opt/spark/work-dir/jobs/"
    "product_benchmark_results.csv"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("full-product-to-stage")
    .getOrCreate()
)

pipeline_start = time.perf_counter()

print("=== SPARK STARTED ===")
print(f"READ PARTITIONS:  {READ_PARTITIONS}")
print(f"WRITE PARTITIONS: {WRITE_PARTITIONS}")


# ============================================================
# BATCH ID
# ============================================================

batch_id = str(uuid.uuid4())

print(f"=== BATCH ID: {batch_id} ===")


# ============================================================
# GET SOURCE BOUNDS
# ============================================================

def get_source_bounds(
    table_name,
    partition_column
):
    """
    Получаем MIN и MAX колонки,
    по которой Spark может делить
    JDBC-чтение на partitions.
    """

    query = f"""
        SELECT
            MIN({partition_column}) AS min_id,
            MAX({partition_column}) AS max_id
        FROM {table_name}
    """

    row = (
        spark.read
        .format("jdbc")
        .option("url", SRC_URL)
        .option("query", query)
        .option("user", SRC_USER)
        .option("password", SRC_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .load()
        .first()
    )

    return (
        int(row["min_id"]),
        int(row["max_id"])
    )


# ============================================================
# JDBC READ FUNCTION
# ============================================================

def read_from_postgres(
    source_query,
    alias,
    partition_column,
    lower_bound,
    upper_bound
):
    """
    Читаем данные из source PostgreSQL.

    При READ_PARTITIONS > 1 Spark
    разбивает диапазон partition_column
    на несколько JDBC partitions.
    """

    reader = (
        spark.read
        .format("jdbc")
        .option("url", SRC_URL)
        .option(
            "dbtable",
            f"({source_query}) AS {alias}"
        )
        .option("user", SRC_USER)
        .option("password", SRC_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .option("fetchsize", "1000")
    )

    if READ_PARTITIONS > 1:
        reader = (
            reader
            .option(
                "partitionColumn",
                partition_column
            )
            .option(
                "lowerBound",
                str(lower_bound)
            )
            .option(
                "upperBound",
                str(upper_bound)
            )
            .option(
                "numPartitions",
                str(READ_PARTITIONS)
            )
        )

    return reader.load()


# ============================================================
# JDBC WRITE FUNCTION
# ============================================================

def write_to_greenplum(
    df,
    target_table
):
    """
    Записываем Spark DataFrame
    в существующую таблицу Greenplum.
    """

    (
        df.write
        .format("jdbc")
        .option("url", GP_URL)
        .option(
            "dbtable",
            target_table
        )
        .option("user", GP_USER)
        .option("password", GP_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )


# ============================================================
# GREENPLUM SQL FUNCTION
# ============================================================

def execute_greenplum_sql(sql):
    """
    Выполняем обычный SQL
    непосредственно в Greenplum.
    """

    connection = None
    statement = None

    try:
        connection = (
            spark
            ._sc
            ._gateway
            .jvm
            .java
            .sql
            .DriverManager
            .getConnection(
                GP_URL,
                GP_USER,
                GP_PASSWORD
            )
        )

        statement = (
            connection.createStatement()
        )

        statement.execute(sql)

    finally:
        if statement is not None:
            statement.close()

        if connection is not None:
            connection.close()


# ============================================================
# GREENPLUM COUNT FUNCTION
# ============================================================

def get_greenplum_count(
    table_name
):
    """
    Получаем количество строк
    из target Greenplum.
    """

    df = (
        spark.read
        .format("jdbc")
        .option("url", GP_URL)
        .option(
            "query",
            f"""
            SELECT COUNT(*) AS cnt
            FROM {table_name}
            """
        )
        .option("user", GP_USER)
        .option("password", GP_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .load()
    )

    return int(
        df.first()["cnt"]
    )


# ============================================================
# SOURCE BOUNDS
# ============================================================

product_lower, product_upper = (
    get_source_bounds(
        "production.product",
        "productid"
    )
)

subcategory_lower, subcategory_upper = (
    get_source_bounds(
        "production.productsubcategory",
        "productsubcategoryid"
    )
)

category_lower, category_upper = (
    get_source_bounds(
        "production.productcategory",
        "productcategoryid"
    )
)


print(
    f"PRODUCT ID RANGE: "
    f"{product_lower} -> {product_upper}"
)

print(
    f"SUBCATEGORY ID RANGE: "
    f"{subcategory_lower} -> "
    f"{subcategory_upper}"
)

print(
    f"CATEGORY ID RANGE: "
    f"{category_lower} -> "
    f"{category_upper}"
)


# ============================================================
# PRODUCT
# ============================================================

product_query = """
SELECT
    productid,
    name,
    productnumber,
    color,
    size,
    standardcost,
    listprice,
    productsubcategoryid,
    modifieddate
FROM production.product
"""

product_df = read_from_postgres(
    source_query=product_query,
    alias="src_product",
    partition_column="productid",
    lower_bound=product_lower,
    upper_bound=product_upper
)

product_stage_df = (
    product_df
    .withColumn(
        "_load_dttm",
        current_timestamp()
    )
    .withColumn(
        "_batch_id",
        lit(batch_id)
    )
    .withColumn(
        "_source_system",
        lit(SOURCE_SYSTEM)
    )
)

product_stage_df.cache()

product_read_start = (
    time.perf_counter()
)

product_count = (
    product_stage_df.count()
)

product_read_seconds = (
    time.perf_counter()
    - product_read_start
)

product_actual_partitions = (
    product_stage_df
    .rdd
    .getNumPartitions()
)

print(
    f"PRODUCT SOURCE ROWS: "
    f"{product_count}"
)

print(
    f"PRODUCT READ PARTITIONS: "
    f"{product_actual_partitions}"
)

print(
    f"PRODUCT SOURCE -> SPARK TIME: "
    f"{product_read_seconds:.3f} sec"
)


# ============================================================
# PRODUCT SUBCATEGORY
# ============================================================

subcategory_query = """
SELECT
    productsubcategoryid,
    productcategoryid,
    name,
    modifieddate
FROM production.productsubcategory
"""

subcategory_df = read_from_postgres(
    source_query=subcategory_query,
    alias="src_productsubcategory",
    partition_column="productsubcategoryid",
    lower_bound=subcategory_lower,
    upper_bound=subcategory_upper
)

subcategory_stage_df = (
    subcategory_df
    .withColumn(
        "_load_dttm",
        current_timestamp()
    )
    .withColumn(
        "_batch_id",
        lit(batch_id)
    )
    .withColumn(
        "_source_system",
        lit(SOURCE_SYSTEM)
    )
)

subcategory_stage_df.cache()

subcategory_read_start = (
    time.perf_counter()
)

subcategory_count = (
    subcategory_stage_df.count()
)

subcategory_read_seconds = (
    time.perf_counter()
    - subcategory_read_start
)

subcategory_actual_partitions = (
    subcategory_stage_df
    .rdd
    .getNumPartitions()
)

print(
    f"SUBCATEGORY SOURCE ROWS: "
    f"{subcategory_count}"
)

print(
    f"SUBCATEGORY READ PARTITIONS: "
    f"{subcategory_actual_partitions}"
)

print(
    f"SUBCATEGORY SOURCE -> SPARK TIME: "
    f"{subcategory_read_seconds:.3f} sec"
)


# ============================================================
# PRODUCT CATEGORY
# ============================================================

category_query = """
SELECT
    productcategoryid,
    name,
    modifieddate
FROM production.productcategory
"""

category_df = read_from_postgres(
    source_query=category_query,
    alias="src_productcategory",
    partition_column="productcategoryid",
    lower_bound=category_lower,
    upper_bound=category_upper
)

category_stage_df = (
    category_df
    .withColumn(
        "_load_dttm",
        current_timestamp()
    )
    .withColumn(
        "_batch_id",
        lit(batch_id)
    )
    .withColumn(
        "_source_system",
        lit(SOURCE_SYSTEM)
    )
)

category_stage_df.cache()

category_read_start = (
    time.perf_counter()
)

category_count = (
    category_stage_df.count()
)

category_read_seconds = (
    time.perf_counter()
    - category_read_start
)

category_actual_partitions = (
    category_stage_df
    .rdd
    .getNumPartitions()
)

print(
    f"CATEGORY SOURCE ROWS: "
    f"{category_count}"
)

print(
    f"CATEGORY READ PARTITIONS: "
    f"{category_actual_partitions}"
)

print(
    f"CATEGORY SOURCE -> SPARK TIME: "
    f"{category_read_seconds:.3f} sec"
)


# ============================================================
# FULL LOAD - TRUNCATE
# ============================================================

truncate_start = (
    time.perf_counter()
)

execute_greenplum_sql(
    "TRUNCATE TABLE stage.product"
)

print(
    "=== TRUNCATED: "
    "stage.product ==="
)

execute_greenplum_sql(
    "TRUNCATE TABLE "
    "stage.productsubcategory"
)

print(
    "=== TRUNCATED: "
    "stage.productsubcategory ==="
)

execute_greenplum_sql(
    "TRUNCATE TABLE "
    "stage.productcategory"
)

print(
    "=== TRUNCATED: "
    "stage.productcategory ==="
)

truncate_seconds = (
    time.perf_counter()
    - truncate_start
)

print(
    f"TRUNCATE TIME: "
    f"{truncate_seconds:.3f} sec"
)


# ============================================================
# PREPARE WRITE PARTITIONS
# ============================================================

product_write_df = (
    product_stage_df
    .repartition(
        WRITE_PARTITIONS
    )
)

subcategory_write_df = (
    subcategory_stage_df
    .repartition(
        WRITE_PARTITIONS
    )
)

category_write_df = (
    category_stage_df
    .repartition(
        WRITE_PARTITIONS
    )
)


print(
    f"PRODUCT WRITE PARTITIONS: "
    f"{product_write_df.rdd.getNumPartitions()}"
)

print(
    f"SUBCATEGORY WRITE PARTITIONS: "
    f"{subcategory_write_df.rdd.getNumPartitions()}"
)

print(
    f"CATEGORY WRITE PARTITIONS: "
    f"{category_write_df.rdd.getNumPartitions()}"
)


# ============================================================
# WRITE PRODUCT
# ============================================================

product_write_start = (
    time.perf_counter()
)

write_to_greenplum(
    product_write_df,
    "stage.product"
)

product_write_seconds = (
    time.perf_counter()
    - product_write_start
)

print(
    "=== PRODUCT LOADED ==="
)

print(
    f"PRODUCT SPARK -> GP TIME: "
    f"{product_write_seconds:.3f} sec"
)


# ============================================================
# WRITE PRODUCT SUBCATEGORY
# ============================================================

subcategory_write_start = (
    time.perf_counter()
)

write_to_greenplum(
    subcategory_write_df,
    "stage.productsubcategory"
)

subcategory_write_seconds = (
    time.perf_counter()
    - subcategory_write_start
)

print(
    "=== PRODUCTSUBCATEGORY LOADED ==="
)

print(
    f"SUBCATEGORY SPARK -> GP TIME: "
    f"{subcategory_write_seconds:.3f} sec"
)


# ============================================================
# WRITE PRODUCT CATEGORY
# ============================================================

category_write_start = (
    time.perf_counter()
)

write_to_greenplum(
    category_write_df,
    "stage.productcategory"
)

category_write_seconds = (
    time.perf_counter()
    - category_write_start
)

print(
    "=== PRODUCTCATEGORY LOADED ==="
)

print(
    f"CATEGORY SPARK -> GP TIME: "
    f"{category_write_seconds:.3f} sec"
)


# ============================================================
# DATA QUALITY / VALIDATION
# ============================================================

validation_start = (
    time.perf_counter()
)

product_target_count = (
    get_greenplum_count(
        "stage.product"
    )
)

subcategory_target_count = (
    get_greenplum_count(
        "stage.productsubcategory"
    )
)

category_target_count = (
    get_greenplum_count(
        "stage.productcategory"
    )
)

validation_seconds = (
    time.perf_counter()
    - validation_start
)


# ============================================================
# COUNT CHECKS
# ============================================================

product_count_ok = (
    product_count
    == product_target_count
)

subcategory_count_ok = (
    subcategory_count
    == subcategory_target_count
)

category_count_ok = (
    category_count
    == category_target_count
)


print("")
print(
    "============================================"
)
print(
    "DATA QUALITY CHECK"
)
print(
    "============================================"
)

print(
    f"PRODUCT: "
    f"source={product_count}, "
    f"target={product_target_count}, "
    f"OK={product_count_ok}"
)

print(
    f"SUBCATEGORY: "
    f"source={subcategory_count}, "
    f"target={subcategory_target_count}, "
    f"OK={subcategory_count_ok}"
)

print(
    f"CATEGORY: "
    f"source={category_count}, "
    f"target={category_target_count}, "
    f"OK={category_count_ok}"
)

print(
    f"VALIDATION TIME: "
    f"{validation_seconds:.3f} sec"
)


# ============================================================
# FAIL PIPELINE IF VALIDATION FAILED
# ============================================================

if not product_count_ok:
    raise RuntimeError(
        "PRODUCT row count mismatch"
    )

if not subcategory_count_ok:
    raise RuntimeError(
        "PRODUCTSUBCATEGORY "
        "row count mismatch"
    )

if not category_count_ok:
    raise RuntimeError(
        "PRODUCTCATEGORY "
        "row count mismatch"
    )


# ============================================================
# TOTAL TIME
# ============================================================

pipeline_seconds = (
    time.perf_counter()
    - pipeline_start
)


# ============================================================
# RESULT
# ============================================================

print("")
print(
    "============================================"
)
print(
    "PRODUCT PIPELINE RESULT"
)
print(
    "============================================"
)

print(
    f"BATCH ID: "
    f"{batch_id}"
)

print(
    f"READ PARTITIONS: "
    f"{READ_PARTITIONS}"
)

print(
    f"WRITE PARTITIONS: "
    f"{WRITE_PARTITIONS}"
)

print(
    f"PRODUCT ROWS: "
    f"{product_count}"
)

print(
    f"SUBCATEGORY ROWS: "
    f"{subcategory_count}"
)

print(
    f"CATEGORY ROWS: "
    f"{category_count}"
)

print(
    f"PRODUCT READ TIME: "
    f"{product_read_seconds:.3f} sec"
)

print(
    f"SUBCATEGORY READ TIME: "
    f"{subcategory_read_seconds:.3f} sec"
)

print(
    f"CATEGORY READ TIME: "
    f"{category_read_seconds:.3f} sec"
)

print(
    f"TRUNCATE TIME: "
    f"{truncate_seconds:.3f} sec"
)

print(
    f"PRODUCT WRITE TIME: "
    f"{product_write_seconds:.3f} sec"
)

print(
    f"SUBCATEGORY WRITE TIME: "
    f"{subcategory_write_seconds:.3f} sec"
)

print(
    f"CATEGORY WRITE TIME: "
    f"{category_write_seconds:.3f} sec"
)

print(
    f"VALIDATION TIME: "
    f"{validation_seconds:.3f} sec"
)

print(
    f"TOTAL PIPELINE TIME: "
    f"{pipeline_seconds:.3f} sec"
)

print(
    "============================================"
)


# ============================================================
# SAVE BENCHMARK RESULT TO CSV
# ============================================================

file_exists = os.path.exists(
    BENCHMARK_FILE
)

with open(
    BENCHMARK_FILE,
    "a",
    newline=""
) as file:

    writer = csv.writer(file)

    if not file_exists:
        writer.writerow([
            "batch_id",
            "read_partitions",
            "write_partitions",
            "product_rows",
            "subcategory_rows",
            "category_rows",
            "product_read_seconds",
            "subcategory_read_seconds",
            "category_read_seconds",
            "truncate_seconds",
            "product_write_seconds",
            "subcategory_write_seconds",
            "category_write_seconds",
            "validation_seconds",
            "total_pipeline_seconds",
            "product_count_ok",
            "subcategory_count_ok",
            "category_count_ok"
        ])

    writer.writerow([
        batch_id,
        READ_PARTITIONS,
        WRITE_PARTITIONS,
        product_count,
        subcategory_count,
        category_count,
        round(
            product_read_seconds,
            3
        ),
        round(
            subcategory_read_seconds,
            3
        ),
        round(
            category_read_seconds,
            3
        ),
        round(
            truncate_seconds,
            3
        ),
        round(
            product_write_seconds,
            3
        ),
        round(
            subcategory_write_seconds,
            3
        ),
        round(
            category_write_seconds,
            3
        ),
        round(
            validation_seconds,
            3
        ),
        round(
            pipeline_seconds,
            3
        ),
        product_count_ok,
        subcategory_count_ok,
        category_count_ok
    ])

print(
    f"BENCHMARK SAVED TO: "
    f"{BENCHMARK_FILE}"
)


# ============================================================
# CLEANUP
# ============================================================

product_stage_df.unpersist()
subcategory_stage_df.unpersist()
category_stage_df.unpersist()

spark.stop()
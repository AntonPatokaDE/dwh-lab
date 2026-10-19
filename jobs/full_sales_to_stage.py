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

# Параметры нашего benchmark
READ_PARTITIONS = int(os.environ.get("READ_PARTITIONS", "1"))
WRITE_PARTITIONS = int(os.environ.get("WRITE_PARTITIONS", "1"))

BENCHMARK_FILE = "/opt/spark/work-dir/jobs/benchmark_results.csv"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("full-sales-to-stage-benchmark")
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

def get_source_bounds(table_name, partition_column):
    """
    Получаем MIN и MAX колонки, по которой Spark будет
    делить JDBC-чтение на partitions.
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
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )

    return int(row["min_id"]), int(row["max_id"])


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
    Читаем PostgreSQL через JDBC.

    READ_PARTITIONS = 1:
        один JDBC partition.

    READ_PARTITIONS > 1:
        Spark делит диапазон partition_column
        на несколько частей и может читать параллельно.
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
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
    )

    if READ_PARTITIONS > 1:
        reader = (
            reader
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(READ_PARTITIONS))
        )

    return reader.load()


# ============================================================
# JDBC WRITE FUNCTION
# ============================================================

def write_to_greenplum(df, target_table):
    """
    Записываем DataFrame в Greenplum.

    Количество JDBC writers примерно определяется
    количеством partitions DataFrame.
    """

    (
        df.write
        .format("jdbc")
        .option("url", GP_URL)
        .option("dbtable", target_table)
        .option("user", GP_USER)
        .option("password", GP_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )


# ============================================================
# GREENPLUM SQL FUNCTION
# ============================================================

def execute_greenplum_sql(sql):
    connection = None
    statement = None

    try:
        connection = (
            spark._sc._gateway.jvm.java.sql.DriverManager
            .getConnection(
                GP_URL,
                GP_USER,
                GP_PASSWORD
            )
        )

        statement = connection.createStatement()
        statement.execute(sql)

    finally:
        if statement is not None:
            statement.close()

        if connection is not None:
            connection.close()


# ============================================================
# GREENPLUM COUNT FUNCTION
# ============================================================

def get_greenplum_count(table_name):
    """
    Проверка количества строк после загрузки.
    """

    df = (
        spark.read
        .format("jdbc")
        .option("url", GP_URL)
        .option(
            "query",
            f"SELECT COUNT(*) AS cnt FROM {table_name}"
        )
        .option("user", GP_USER)
        .option("password", GP_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return int(df.first()["cnt"])


# ============================================================
# SOURCE BOUNDS
# ============================================================

header_lower, header_upper = get_source_bounds(
    "sales.salesorderheader",
    "salesorderid"
)

detail_lower, detail_upper = get_source_bounds(
    "sales.salesorderdetail",
    "salesorderdetailid"
)

print(
    f"HEADER ID RANGE: {header_lower} -> {header_upper}"
)

print(
    f"DETAIL ID RANGE: {detail_lower} -> {detail_upper}"
)


# ============================================================
# SALES ORDER HEADER
# ============================================================

header_query = """
SELECT
    salesorderid,
    orderdate,
    status,
    onlineorderflag,
    customerid,
    territoryid,
    subtotal,
    totaldue,
    modifieddate
FROM sales.salesorderheader
"""

header_df = read_from_postgres(
    source_query=header_query,
    alias="src_header",
    partition_column="salesorderid",
    lower_bound=header_lower,
    upper_bound=header_upper
)

header_stage_df = (
    header_df
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

# Говорим Spark сохранить материализованный DataFrame.
header_stage_df.cache()

header_read_start = time.perf_counter()

# count() — action.
# Именно здесь Spark реально читает PostgreSQL.
header_count = header_stage_df.count()

header_read_seconds = (
    time.perf_counter()
    - header_read_start
)

header_actual_partitions = (
    header_stage_df.rdd.getNumPartitions()
)

print(f"HEADER SOURCE ROWS: {header_count}")
print(
    f"HEADER READ PARTITIONS: "
    f"{header_actual_partitions}"
)
print(
    f"HEADER SOURCE -> SPARK TIME: "
    f"{header_read_seconds:.3f} sec"
)


# ============================================================
# SALES ORDER DETAIL
# ============================================================

detail_query = """
SELECT
    salesorderid,
    salesorderdetailid,
    orderqty,
    productid,
    unitprice,
    unitpricediscount,
    modifieddate
FROM sales.salesorderdetail
"""

detail_df = read_from_postgres(
    source_query=detail_query,
    alias="src_detail",
    partition_column="salesorderdetailid",
    lower_bound=detail_lower,
    upper_bound=detail_upper
)

detail_stage_df = (
    detail_df
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

detail_stage_df.cache()

detail_read_start = time.perf_counter()

detail_count = detail_stage_df.count()

detail_read_seconds = (
    time.perf_counter()
    - detail_read_start
)

detail_actual_partitions = (
    detail_stage_df.rdd.getNumPartitions()
)

print(f"DETAIL SOURCE ROWS: {detail_count}")
print(
    f"DETAIL READ PARTITIONS: "
    f"{detail_actual_partitions}"
)
print(
    f"DETAIL SOURCE -> SPARK TIME: "
    f"{detail_read_seconds:.3f} sec"
)


# ============================================================
# FULL LOAD - TRUNCATE
# ============================================================

truncate_start = time.perf_counter()

execute_greenplum_sql(
    "TRUNCATE TABLE stage.salesorderheader"
)

print(
    "=== TRUNCATED: "
    "stage.salesorderheader ==="
)

execute_greenplum_sql(
    "TRUNCATE TABLE stage.salesorderdetail"
)

print(
    "=== TRUNCATED: "
    "stage.salesorderdetail ==="
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

header_write_df = (
    header_stage_df
    .repartition(WRITE_PARTITIONS)
)

detail_write_df = (
    detail_stage_df
    .repartition(WRITE_PARTITIONS)
)

print(
    f"HEADER WRITE PARTITIONS: "
    f"{header_write_df.rdd.getNumPartitions()}"
)

print(
    f"DETAIL WRITE PARTITIONS: "
    f"{detail_write_df.rdd.getNumPartitions()}"
)


# ============================================================
# WRITE HEADER
# ============================================================

header_write_start = time.perf_counter()

write_to_greenplum(
    header_write_df,
    "stage.salesorderheader"
)

header_write_seconds = (
    time.perf_counter()
    - header_write_start
)

print("=== HEADER LOADED ===")
print(
    f"HEADER SPARK -> GP TIME: "
    f"{header_write_seconds:.3f} sec"
)


# ============================================================
# WRITE DETAIL
# ============================================================

detail_write_start = time.perf_counter()

write_to_greenplum(
    detail_write_df,
    "stage.salesorderdetail"
)

detail_write_seconds = (
    time.perf_counter()
    - detail_write_start
)

print("=== DETAIL LOADED ===")
print(
    f"DETAIL SPARK -> GP TIME: "
    f"{detail_write_seconds:.3f} sec"
)


# ============================================================
# DATA QUALITY / VALIDATION
# ============================================================

validation_start = time.perf_counter()

header_target_count = get_greenplum_count(
    "stage.salesorderheader"
)

detail_target_count = get_greenplum_count(
    "stage.salesorderdetail"
)

validation_seconds = (
    time.perf_counter()
    - validation_start
)

header_count_ok = (
    header_count == header_target_count
)

detail_count_ok = (
    detail_count == detail_target_count
)

print("")
print("=== DATA QUALITY CHECK ===")

print(
    f"HEADER: source={header_count}, "
    f"target={header_target_count}, "
    f"OK={header_count_ok}"
)

print(
    f"DETAIL: source={detail_count}, "
    f"target={detail_target_count}, "
    f"OK={detail_count_ok}"
)

print(
    f"VALIDATION TIME: "
    f"{validation_seconds:.3f} sec"
)

if not header_count_ok:
    raise RuntimeError(
        "HEADER row count mismatch"
    )

if not detail_count_ok:
    raise RuntimeError(
        "DETAIL row count mismatch"
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
print("============================================")
print("BENCHMARK RESULT")
print("============================================")

print(f"BATCH ID:              {batch_id}")

print(f"READ PARTITIONS:       {READ_PARTITIONS}")
print(f"WRITE PARTITIONS:      {WRITE_PARTITIONS}")

print(f"HEADER ROWS:           {header_count}")
print(f"DETAIL ROWS:           {detail_count}")

print(
    f"HEADER READ TIME:      "
    f"{header_read_seconds:.3f} sec"
)

print(
    f"DETAIL READ TIME:      "
    f"{detail_read_seconds:.3f} sec"
)

print(
    f"TRUNCATE TIME:         "
    f"{truncate_seconds:.3f} sec"
)

print(
    f"HEADER WRITE TIME:     "
    f"{header_write_seconds:.3f} sec"
)

print(
    f"DETAIL WRITE TIME:     "
    f"{detail_write_seconds:.3f} sec"
)

print(
    f"VALIDATION TIME:       "
    f"{validation_seconds:.3f} sec"
)

print(
    f"TOTAL PIPELINE TIME:   "
    f"{pipeline_seconds:.3f} sec"
)

print("============================================")


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
            "header_rows",
            "detail_rows",
            "header_read_seconds",
            "detail_read_seconds",
            "truncate_seconds",
            "header_write_seconds",
            "detail_write_seconds",
            "validation_seconds",
            "total_pipeline_seconds",
            "header_count_ok",
            "detail_count_ok"
        ])

    writer.writerow([
        batch_id,
        READ_PARTITIONS,
        WRITE_PARTITIONS,
        header_count,
        detail_count,
        round(
            header_read_seconds,
            3
        ),
        round(
            detail_read_seconds,
            3
        ),
        round(
            truncate_seconds,
            3
        ),
        round(
            header_write_seconds,
            3
        ),
        round(
            detail_write_seconds,
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
        header_count_ok,
        detail_count_ok
    ])

print(
    f"BENCHMARK SAVED TO: "
    f"{BENCHMARK_FILE}"
)


# ============================================================
# CLEANUP
# ============================================================

header_stage_df.unpersist()
detail_stage_df.unpersist()

spark.stop()
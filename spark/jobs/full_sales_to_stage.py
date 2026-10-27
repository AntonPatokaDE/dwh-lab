import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

SOURCE_HOST = os.environ["SOURCE_HOST"]
SOURCE_PORT = os.environ["SOURCE_PORT"]
SOURCE_DB = os.environ["SOURCE_DB"]
SRC_USER = os.environ["SRC_USER"]
SRC_PASSWORD = os.environ["SRC_PASSWORD"]

GP_HOST = os.environ["GP_HOST"]
GP_PORT = os.environ["GP_PORT"]
GP_DB = os.environ["GP_DB"]
GP_USER = os.environ["GP_USER"]
GP_PASSWORD = os.environ["GP_PASSWORD"]

SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "adventureworks")
READ_PARTITIONS = int(os.getenv("READ_PARTITIONS", "1"))
WRITE_PARTITIONS = int(os.getenv("WRITE_PARTITIONS", "1"))

SRC_URL = f"jdbc:postgresql://{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}"
GP_URL = f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}"

spark = SparkSession.builder.appName("full-sales-to-stage").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
batch_id = str(uuid.uuid4())


def get_source_bounds(table_name, partition_column):
    query = f"""
        SELECT MIN({partition_column}) AS min_id,
               MAX({partition_column}) AS max_id
        FROM {table_name}
    """
    row = (
        spark.read.format("jdbc")
        .option("url", SRC_URL)
        .option("query", query)
        .option("user", SRC_USER)
        .option("password", SRC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )
    if row["min_id"] is None or row["max_id"] is None:
        raise RuntimeError(f"Source table {table_name} is empty")
    return int(row["min_id"]), int(row["max_id"])


def read_from_postgres(source_query, alias, partition_column, lower_bound, upper_bound):
    reader = (
        spark.read.format("jdbc")
        .option("url", SRC_URL)
        .option("dbtable", f"({source_query}) AS {alias}")
        .option("user", SRC_USER)
        .option("password", SRC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
    )
    if READ_PARTITIONS > 1:
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(READ_PARTITIONS))
        )
    return reader.load()


def execute_greenplum_sql(sql):
    connection = None
    statement = None
    try:
        connection = (
            spark._sc._gateway.jvm.java.sql.DriverManager
            .getConnection(GP_URL, GP_USER, GP_PASSWORD)
        )
        statement = connection.createStatement()
        statement.execute(sql)
    finally:
        if statement is not None:
            statement.close()
        if connection is not None:
            connection.close()


def write_to_greenplum(df, target_table):
    (
        df.write.format("jdbc")
        .option("url", GP_URL)
        .option("dbtable", target_table)
        .option("user", GP_USER)
        .option("password", GP_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )


try:
    header_lower, header_upper = get_source_bounds(
        "sales.salesorderheader", "salesorderid"
    )
    detail_lower, detail_upper = get_source_bounds(
        "sales.salesorderdetail", "salesorderdetailid"
    )

    header_query = """
        SELECT salesorderid, orderdate, status, onlineorderflag,
               customerid, territoryid, subtotal, totaldue, modifieddate
        FROM sales.salesorderheader
    """

    detail_query = """
        SELECT salesorderid, salesorderdetailid, orderqty, productid,
               unitprice, unitpricediscount, modifieddate
        FROM sales.salesorderdetail
    """

    header_df = read_from_postgres(
        header_query, "src_header", "salesorderid", header_lower, header_upper
    )
    detail_df = read_from_postgres(
        detail_query, "src_detail", "salesorderdetailid", detail_lower, detail_upper
    )

    header_stage_df = (
        header_df
        .withColumn("_load_dttm", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
    )

    detail_stage_df = (
        detail_df
        .withColumn("_load_dttm", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
    )

    execute_greenplum_sql("TRUNCATE TABLE stage.salesorderheader")
    execute_greenplum_sql("TRUNCATE TABLE stage.salesorderdetail")

    if WRITE_PARTITIONS > 0:
        header_stage_df = header_stage_df.repartition(WRITE_PARTITIONS)
        detail_stage_df = detail_stage_df.repartition(WRITE_PARTITIONS)

    write_to_greenplum(header_stage_df, "stage.salesorderheader")
    write_to_greenplum(detail_stage_df, "stage.salesorderdetail")

    print(f"SALES LOAD SUCCESS. BATCH_ID={batch_id}")

finally:
    spark.stop()

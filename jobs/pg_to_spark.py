import os

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("postgres-read-test")
    .getOrCreate()
)


print("=== SPARK STARTED ===")


df = (
    spark.read
    .format("jdbc")
    .option("url", os.environ["SRC_URL"])
    .option("dbtable", "sales.salesorderheader")
    .option("user", os.environ["SRC_USER"])
    .option("password", os.environ["SRC_PASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .load()
)


print("=== SCHEMA ===")
df.printSchema()


print("=== FIRST 10 ROWS ===")
df.show(10, truncate=False)


print("=== ROW COUNT ===")
print(df.count())


spark.stop()
import os

from pyspark.sql import SparkSession

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
])
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
WAREHOUSE = os.getenv("WAREHOUSE", os.path.abspath("warehouse"))
CHECKPOINTS = os.getenv("CHECKPOINTS", os.path.abspath("checkpoints"))


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
from pyspark.sql import functions as F

from .common import BOOTSTRAP, CHECKPOINTS, get_spark

TOPICS = {
    "nem.dispatch.price": "price",
    "nem.dispatch.regionsum": "regionsum",
    "nem.dispatch.interconnector": "interconnector",
}

DDL = """
CREATE TABLE IF NOT EXISTS local.bronze.{t} (
  kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_ts timestamp,
  msg_key string, raw_value string, bronze_loaded_at timestamp
) USING iceberg PARTITIONED BY (days(kafka_ts))
"""


def main() -> None:
    spark = get_spark("bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    for topic, t in TOPICS.items():
        spark.sql(DDL.format(t=t))
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        out = raw.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("msg_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.current_timestamp().alias("bronze_loaded_at"),
        )
        (
            out.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime="60 seconds")
            .option("fanout-enabled", "true")
            .option("checkpointLocation", f"{CHECKPOINTS}/bronze_{t}")
            .toTable(f"local.bronze.{t}")
        )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
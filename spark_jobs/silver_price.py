from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import CHECKPOINTS, get_spark

ENVELOPE = (
    "table string, source_file string, ingested_at string, "
    "settlement_nem string, settlement_utc string, "
    "data struct<REGIONID:string,RRP:string,ROP:string,APCFLAG:string,"
    "INTERVENTION:string,DISPATCHINTERVAL:string,PRICE_STATUS:string>"
)
REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
RRP_MIN, RRP_MAX = -1000.0, 30000.0  

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS local.silver.price (
  region_id string, settlement_utc timestamp, intervention int,
  settlement_nem string, dispatch_interval string,
  rrp double, rop double, apcflag int, price_status string,
  source_file string, kafka_offset bigint, ingested_at timestamp
) USING iceberg PARTITIONED BY (days(settlement_utc))
"""
QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS local.silver.price_quarantine (
  kafka_topic string, kafka_partition int, kafka_offset bigint,
  raw_value string, reasons string, quarantined_at timestamp
) USING iceberg
"""
SILVER_COLS = [
    "region_id", "settlement_utc", "intervention", "settlement_nem",
    "dispatch_interval", "rrp", "rop", "apcflag", "price_status",
    "source_file", "kafka_offset", "ingested_at",
]


def parse(df):
    p = df.withColumn("p", F.from_json("raw_value", ENVELOPE))
    return p.select(
        "kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
        F.col("p.data.REGIONID").alias("region_id"),
        F.col("p.settlement_utc").cast("timestamp").alias("settlement_utc"),
        F.col("p.settlement_nem").alias("settlement_nem"),
        F.col("p.data.INTERVENTION").cast("int").alias("intervention"),
        F.col("p.data.DISPATCHINTERVAL").alias("dispatch_interval"),
        F.col("p.data.RRP").cast("double").alias("rrp"),
        F.col("p.data.ROP").cast("double").alias("rop"),
        F.col("p.data.APCFLAG").cast("int").alias("apcflag"),
        F.col("p.data.PRICE_STATUS").alias("price_status"),
        F.col("p.source_file").alias("source_file"),
        F.col("p.ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("p").isNull().alias("unparseable"),
    )


def with_reasons(df):
    reasons = F.concat_ws(
        ",",
        F.when(F.col("unparseable"), F.lit("unparseable_json")),
        F.when(~F.col("region_id").isin(*REGIONS) | F.col("region_id").isNull(),
               F.lit("bad_region")),
        F.when(F.col("settlement_utc").isNull(), F.lit("null_settlement")),
        F.when(F.col("rrp").isNull(), F.lit("null_rrp")),
        F.when((F.col("rrp") < RRP_MIN) | (F.col("rrp") > RRP_MAX),
               F.lit("rrp_out_of_range")),
        F.when(~F.col("intervention").isin(0, 1) | F.col("intervention").isNull(),
               F.lit("bad_intervention")),
    )
    return df.withColumn("reasons", reasons)  


def process(batch_df, batch_id):
    spark = batch_df.sparkSession
    checked = with_reasons(parse(batch_df)).cache()
    good = checked.filter(F.col("reasons") == "")
    bad = checked.filter(F.col("reasons") != "")

    w = Window.partitionBy("region_id", "settlement_utc", "intervention") \
              .orderBy(F.col("kafka_offset").desc())
    (good.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
         .select(*SILVER_COLS).createOrReplaceTempView("batch_good"))

    spark.sql("""
        MERGE INTO local.silver.price t USING batch_good s
        ON  t.region_id = s.region_id
        AND t.settlement_utc = s.settlement_utc
        AND t.intervention = s.intervention
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    n_bad = bad.count()
    if n_bad:
        (bad.select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
                    "reasons", F.current_timestamp().alias("quarantined_at"))
            .createOrReplaceTempView("batch_bad"))
        spark.sql("""
            MERGE INTO local.silver.price_quarantine t USING batch_bad s
            ON  t.kafka_topic = s.kafka_topic
            AND t.kafka_partition = s.kafka_partition
            AND t.kafka_offset = s.kafka_offset
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"[silver_price] batch {batch_id}: good={good.count()} quarantined={n_bad}")
    checked.unpersist()


def main() -> None:
    spark = get_spark("silver_price")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql(SILVER_DDL)
    spark.sql(QUARANTINE_DDL)

    src = spark.readStream.format("iceberg").load("local.bronze.price")
    (
        src.writeStream.foreachBatch(process)
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_price")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
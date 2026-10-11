from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import CHECKPOINTS, get_spark

REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
ENVELOPE = (
    "settlement_utc string, settlement_nem string, source_file string, "
    "ingested_at string, data map<string,string>"
)

CONFIGS = {
    "regionsum": {
        "entity_src": "REGIONID",
        "entity": "region_id",
        "numeric": [
            "TOTALDEMAND", "AVAILABLEGENERATION", "DISPATCHABLEGENERATION",
            "NETINTERCHANGE", "CLEAREDSUPPLY", "DEMANDFORECAST",
            "DEMAND_AND_NONSCHEDGEN", "TOTALINTERMITTENTGENERATION",
            "SS_SOLAR_CLEAREDMW", "SS_SOLAR_AVAILABILITY",
            "SS_WIND_CLEAREDMW", "SS_WIND_AVAILABILITY",
            "BDU_CLEAREDMW_GEN", "BDU_CLEAREDMW_LOAD",
            "LORSURPLUS", "LRCSURPLUS",
        ],
        "checks": [
            ("bad_region", lambda: F.coalesce(~F.col("region_id").isin(*REGIONS), F.lit(True))),
            ("null_demand", lambda: F.col("totaldemand").isNull()),
            ("demand_out_of_range",
             lambda: (F.col("totaldemand") < -5000) | (F.col("totaldemand") > 40000)),
        ],
    },
    "interconnector": {
        "entity_src": "INTERCONNECTORID",
        "entity": "interconnector_id",
        "numeric": [
            "METEREDMWFLOW", "MWFLOW", "MWLOSSES",
            "EXPORTLIMIT", "IMPORTLIMIT", "MARGINALVALUE",
            "VIOLATIONDEGREE",
        ],
        "checks": [
            ("null_interconnector", lambda: F.col("interconnector_id").isNull()),
            ("null_flow", lambda: F.col("mwflow").isNull()),
            ("flow_out_of_range", lambda: F.abs(F.col("mwflow")) > 10000),
        ],
    },
}

QUAR_DDL = """
CREATE TABLE IF NOT EXISTS local.silver.{name}_quarantine (
  kafka_topic string, kafka_partition int, kafka_offset bigint,
  raw_value string, reasons string, quarantined_at timestamp
) USING iceberg
"""


def ddl(name, cfg):
    num = ", ".join(f"{c.lower()} double" for c in cfg["numeric"])
    return f"""
    CREATE TABLE IF NOT EXISTS local.silver.{name} (
      {cfg['entity']} string, settlement_utc timestamp, intervention int,
      settlement_nem string, {num},
      source_file string, kafka_offset bigint, ingested_at timestamp
    ) USING iceberg PARTITIONED BY (days(settlement_utc))
    """


def parse(df, cfg):
    p = df.withColumn("p", F.from_json("raw_value", ENVELOPE))
    cols = [
        "kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
        F.col("p.data")[cfg["entity_src"]].alias(cfg["entity"]),
        F.col("p.settlement_utc").cast("timestamp").alias("settlement_utc"),
        F.col("p.settlement_nem").alias("settlement_nem"),
        F.col("p.data")["INTERVENTION"].cast("int").alias("intervention"),
        F.col("p.source_file").alias("source_file"),
        F.col("p.ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("p").isNull().alias("unparseable"),
    ]
    cols += [F.col("p.data")[c].cast("double").alias(c.lower()) for c in cfg["numeric"]]
    return p.select(*cols)


def with_reasons(df, cfg):
    rules = [
        ("unparseable_json", F.col("unparseable")),
        ("null_settlement", F.col("settlement_utc").isNull()),
        ("bad_intervention",
         F.coalesce(~F.col("intervention").isin(0, 1), F.lit(True))),
    ] + [(reason, check()) for reason, check in cfg["checks"]]
    reasons = F.concat_ws(",", *[F.when(cond, F.lit(r)) for r, cond in rules])
    return df.withColumn("reasons", reasons)


def make_processor(name, cfg):
    key = [cfg["entity"], "settlement_utc", "intervention"]
    silver_cols = (
        [cfg["entity"], "settlement_utc", "intervention", "settlement_nem"]
        + [c.lower() for c in cfg["numeric"]]
        + ["source_file", "kafka_offset", "ingested_at"]
    )

    def process(batch_df, batch_id):
        spark = batch_df.sparkSession
        checked = with_reasons(parse(batch_df, cfg), cfg).cache()
        good = checked.filter(F.col("reasons") == "")
        bad = checked.filter(F.col("reasons") != "")

        w = Window.partitionBy(*key).orderBy(F.col("kafka_offset").desc())
        good_view, bad_view = f"batch_good_{name}", f"batch_bad_{name}"
        (good.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
             .select(*silver_cols).createOrReplaceTempView(good_view))
        on = " AND ".join(f"t.{k} = s.{k}" for k in key)
        spark.sql(f"""
            MERGE INTO local.silver.{name} t USING {good_view} s ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        n_bad = bad.count()
        if n_bad:
            (bad.select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
                        "reasons", F.current_timestamp().alias("quarantined_at"))
                .createOrReplaceTempView(bad_view))
            spark.sql(f"""
                MERGE INTO local.silver.{name}_quarantine t USING {bad_view} s
                ON  t.kafka_topic = s.kafka_topic
                AND t.kafka_partition = s.kafka_partition
                AND t.kafka_offset = s.kafka_offset
                WHEN NOT MATCHED THEN INSERT *
            """)
        print(f"[silver_{name}] batch {batch_id}: good={good.count()} quarantined={n_bad}")
        checked.unpersist()

    return process


def main() -> None:
    spark = get_spark("silver_other")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    for name, cfg in CONFIGS.items():
        spark.sql(ddl(name, cfg))
        spark.sql(QUAR_DDL.format(name=name))
        (
            spark.readStream.format("iceberg").load(f"local.bronze.{name}")
            .writeStream.foreachBatch(make_processor(name, cfg))
            .trigger(processingTime="60 seconds")
            .option("checkpointLocation", f"{CHECKPOINTS}/silver_{name}")
            .start()
        )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
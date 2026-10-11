from pathlib import Path

from .common import get_spark

REGIMES_CSV = str(Path("reference/price_regimes.csv").resolve())

DDL = """
CREATE TABLE IF NOT EXISTS local.gold.region_interval (
  region_id string, settlement_utc timestamp, settlement_nem string, market_day date,
  rrp double, apcflag int, price_regime string,
  totaldemand double, availablegeneration double, dispatchablegeneration double,
  netinterchange double, demand_and_nonschedgen double, totalintermittentgeneration double,
  ss_solar_clearedmw double, ss_wind_clearedmw double,
  ss_solar_availability double, ss_wind_availability double,
  curtailment_ratio double
) USING iceberg PARTITIONED BY (days(settlement_utc))
"""

BUILD = """
WITH reg AS (
  SELECT region_id, regime,
         to_timestamp(from_nem, 'yyyy/MM/dd HH:mm:ss') AS t_from,
         to_timestamp(to_nem,   'yyyy/MM/dd HH:mm:ss') AS t_to,
         CASE WHEN regime LIKE 'suspended%' THEN 1 ELSE 2 END AS prio
  FROM regimes
),
base AS (
  SELECT p.region_id, p.settlement_utc, p.settlement_nem,
         to_timestamp(p.settlement_nem, 'yyyy/MM/dd HH:mm:ss') AS t_nem,
         p.rrp, p.apcflag,
         r.totaldemand, r.availablegeneration, r.dispatchablegeneration,
         r.netinterchange, r.demand_and_nonschedgen, r.totalintermittentgeneration,
         r.ss_solar_clearedmw, r.ss_wind_clearedmw,
         r.ss_solar_availability, r.ss_wind_availability
  FROM local.silver.price p
  JOIN local.silver.regionsum r
    ON p.region_id = r.region_id AND p.settlement_utc = r.settlement_utc
  WHERE p.intervention = 0 AND r.intervention = 0
),
ranked AS (
  SELECT b.*, g.regime,
         row_number() OVER (PARTITION BY b.region_id, b.settlement_utc ORDER BY g.prio) AS rn
  FROM base b
  LEFT JOIN reg g
    ON b.region_id = g.region_id AND b.t_nem BETWEEN g.t_from AND g.t_to
)
SELECT region_id, settlement_utc, settlement_nem,
       to_date(t_nem - INTERVAL 245 MINUTES) AS market_day,
       rrp, apcflag, coalesce(regime, 'market') AS price_regime,
       totaldemand, availablegeneration, dispatchablegeneration,
       netinterchange, demand_and_nonschedgen, totalintermittentgeneration,
       ss_solar_clearedmw, ss_wind_clearedmw,
       ss_solar_availability, ss_wind_availability,
       CASE WHEN ss_solar_availability IS NOT NULL AND ss_wind_availability IS NOT NULL
             AND ss_solar_availability + ss_wind_availability > 0
            THEN 1 - (coalesce(ss_solar_clearedmw, 0) + coalesce(ss_wind_clearedmw, 0))
                     / (ss_solar_availability + ss_wind_availability)
       END AS curtailment_ratio
FROM ranked WHERE rn = 1
"""


def main() -> None:
    spark = get_spark("gold_region_interval")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    spark.sql(DDL)
    spark.read.option("header", True).csv(REGIMES_CSV).createOrReplaceTempView("regimes")
    spark.sql(BUILD).createOrReplaceTempView("gold_build")
    spark.sql("""
        MERGE INTO local.gold.region_interval t USING gold_build s
        ON t.region_id = s.region_id AND t.settlement_utc = s.settlement_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = spark.sql("SELECT count(*) AS n FROM local.gold.region_interval").first().n
    print(f"gold.region_interval rows: {n}")


if __name__ == "__main__":
    main()
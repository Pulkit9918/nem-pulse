from spark_jobs.common import get_spark

LO, HI = "2022/06/01 00:05:00", "2022/07/01 00:00:00"
june = f"settlement_nem BETWEEN '{LO}' AND '{HI}'"

spark = get_spark("check_gold")

print("1. June 2022: rows vs distinct keys")
spark.sql(f"""
  SELECT count(*) AS rows, count(DISTINCT region_id, settlement_utc) AS distinct_keys
  FROM local.gold.region_interval WHERE {june}
""").show()

print("2. Rows per market day")
spark.sql(f"""
  SELECT market_day, count(*) AS rows, count(DISTINCT region_id) AS regions
  FROM local.gold.region_interval WHERE {june}
  GROUP BY market_day ORDER BY market_day
""").show(40)

print("3. Price by regime")
spark.sql(f"""
  SELECT price_regime, count(*) AS rows, round(min(rrp), 1) AS min_rrp,
         round(avg(rrp), 1) AS mean_rrp, round(max(rrp), 1) AS max_rrp
  FROM local.gold.region_interval WHERE {june}
  GROUP BY price_regime ORDER BY price_regime
""").show()

print("4. curtailment_ratio coverage: backfill vs live")
spark.sql("""
  SELECT CASE WHEN settlement_nem >= '2026/10/01' THEN 'live' ELSE 'backfill' END AS src,
         count(*) AS rows, count(curtailment_ratio) AS with_ratio,
         round(min(curtailment_ratio), 3) AS min_ratio, round(max(curtailment_ratio), 3) AS max_ratio
  FROM local.gold.region_interval GROUP BY 1
""").show()
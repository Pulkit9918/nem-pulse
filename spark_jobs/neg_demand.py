from .common import get_spark

spark = get_spark("neg_demand")
spark.sql("""
    SELECT r.settlement_utc, r.totaldemand, r.ss_solar_clearedmw, r.ss_wind_clearedmw, p.rrp
    FROM local.silver.regionsum r
    JOIN local.silver.price p
      ON r.region_id = p.region_id
     AND r.settlement_utc = p.settlement_utc
     AND r.intervention = p.intervention
    WHERE r.region_id = 'SA1' AND r.totaldemand < 0
    ORDER BY r.settlement_utc
""").show(40, truncate=False)
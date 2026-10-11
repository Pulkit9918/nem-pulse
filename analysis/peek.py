from spark_jobs.common import get_spark

spark = get_spark("peek")
spark.sql("""
    SELECT 'bronze.price' AS tbl, count(*) AS n FROM local.bronze.price
    UNION ALL SELECT 'bronze.regionsum', count(*) FROM local.bronze.regionsum
    UNION ALL SELECT 'bronze.interconnector', count(*) FROM local.bronze.interconnector
    UNION ALL SELECT 'silver.price', count(*) FROM local.silver.price
    UNION ALL SELECT 'silver.price_quarantine', count(*) FROM local.silver.price_quarantine
""").show(truncate=False)
spark.sql("""
    SELECT region_id, settlement_utc, rrp, intervention
    FROM local.silver.price ORDER BY settlement_utc DESC, region_id LIMIT 10
""").show(truncate=False)
spark.sql("SELECT reasons, raw_value FROM local.silver.price_quarantine").show(truncate=60)
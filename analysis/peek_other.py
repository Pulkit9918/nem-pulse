from spark_jobs.common import get_spark

spark = get_spark("peek_other")
spark.sql("""
    SELECT region_id, settlement_utc, totaldemand, availablegeneration, netinterchange
    FROM local.silver.regionsum ORDER BY settlement_utc DESC, region_id LIMIT 10
""").show(truncate=False)
spark.sql("""
    SELECT interconnector_id, settlement_utc, mwflow, mwlosses, exportlimit, importlimit
    FROM local.silver.interconnector ORDER BY settlement_utc DESC, interconnector_id LIMIT 14
""").show(truncate=False)
spark.sql("""
    SELECT 'regionsum' t, count(*) n FROM local.silver.regionsum
    UNION ALL SELECT 'regionsum_quarantine', count(*) FROM local.silver.regionsum_quarantine
    UNION ALL SELECT 'interconnector', count(*) FROM local.silver.interconnector
    UNION ALL SELECT 'interconnector_quarantine', count(*) FROM local.silver.interconnector_quarantine
""").show(truncate=False)
from spark_jobs.common import get_spark

spark = get_spark("solar_check")
spark.sql("""
    SELECT region_id,
           max(ss_solar_clearedmw)  AS max_solar,
           max(ss_wind_clearedmw)   AS max_wind,
           max(ss_solar_availability) AS max_solar_avail,
           sum(CASE WHEN ss_solar_clearedmw > 0 THEN 1 ELSE 0 END) AS solar_nonzero_rows
    FROM local.silver.regionsum GROUP BY region_id ORDER BY region_id
""").show()
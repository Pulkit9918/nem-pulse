from spark_jobs.common import get_spark

spark = get_spark("qcheck_other")
spark.sql("""
    SELECT reasons, count(*) AS n
    FROM local.silver.regionsum_quarantine GROUP BY reasons ORDER BY n DESC
""").show(truncate=False)
spark.sql("""
    SELECT kafka_offset,
           get_json_object(raw_value, '$.settlement_nem')      AS settlement_nem,
           get_json_object(raw_value, '$.data.REGIONID')       AS region,
           get_json_object(raw_value, '$.data.INTERVENTION')   AS intervention,
           get_json_object(raw_value, '$.data.TOTALDEMAND')    AS totaldemand,
           reasons
    FROM local.silver.regionsum_quarantine
    ORDER BY kafka_offset LIMIT 25
""").show(truncate=False)
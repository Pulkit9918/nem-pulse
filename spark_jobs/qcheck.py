from .common import get_spark

spark = get_spark("qcheck")
spark.sql("""
    SELECT kafka_partition, kafka_offset, count(*) AS copies
    FROM local.silver.price_quarantine
    GROUP BY 1, 2 ORDER BY 1, 2
""").show()
spark.sql("SELECT count(*) AS xx9_rows_in_silver FROM local.silver.price WHERE region_id = 'XX9'").show()
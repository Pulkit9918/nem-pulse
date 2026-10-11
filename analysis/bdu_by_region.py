from spark_jobs.common import get_spark

spark = get_spark("bdu_by_region")
spark.sql("""
    SELECT region_id, count(*) AS rows, count(bdu_clearedmw_gen) AS with_bdu_gen,
           round(min(bdu_clearedmw_gen), 1) AS min_gen, round(max(bdu_clearedmw_gen), 1) AS max_gen
    FROM local.silver.regionsum
    WHERE settlement_nem >= '2026/10/01'
    GROUP BY region_id ORDER BY region_id
""").show()
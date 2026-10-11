from spark_jobs.common import get_spark

spark = get_spark("dupes")
print("bronze duplicate keys (expected > 0 after replay):")
spark.sql("""
    SELECT count(*) AS dup_keys FROM (
      SELECT msg_key, count(*) c FROM local.bronze.price GROUP BY msg_key HAVING c > 1)
""").show()
print("silver duplicate keys (must be 0):")
spark.sql("""
    SELECT count(*) AS dup_keys FROM (
      SELECT region_id, settlement_utc, intervention, count(*) c
      FROM local.silver.price GROUP BY 1, 2, 3 HAVING c > 1)
""").show()
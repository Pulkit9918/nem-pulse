from .common import get_spark

spark = get_spark("reprocess")
spark.sql("DELETE FROM local.silver.regionsum_quarantine WHERE reasons = 'demand_out_of_range'")
spark.sql("SELECT count(*) AS quarantine_rows_left FROM local.silver.regionsum_quarantine").show()
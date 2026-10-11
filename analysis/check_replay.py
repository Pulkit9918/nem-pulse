import sys
from datetime import datetime, timedelta

from spark_jobs.common import get_spark

day = sys.argv[1] if len(sys.argv) > 1 else "2022/06/13"
d0 = datetime.strptime(day, "%Y/%m/%d")
lo = d0.strftime("%Y/%m/%d") + " 00:05:00"
hi = (d0 + timedelta(days=1)).strftime("%Y/%m/%d") + " 00:00:00"

spark = get_spark("check_replay")
for t in ("price", "regionsum", "interconnector"):
    spark.sql(f"""
        SELECT '{t}' AS tbl, intervention, count(*) AS n
        FROM local.silver.{t}
        WHERE settlement_nem BETWEEN '{lo}' AND '{hi}'
        GROUP BY intervention ORDER BY intervention
    """).show()
    spark.sql(f"""
        SELECT '{t}' AS tbl, count(*) AS quarantined_backfill_rows
        FROM local.silver.{t}_quarantine WHERE raw_value LIKE '%mmsdm_backfill%'
    """).show()
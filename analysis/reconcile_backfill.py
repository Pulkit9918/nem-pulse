from pathlib import Path

from spark_jobs.common import get_spark

LO, HI = "2022/06/01 00:05:00", "2022/07/01 00:00:00"
TABLES = {
    "price": ("DISPATCHPRICE", "region_id"),
    "regionsum": ("DISPATCHREGIONSUM", "region_id"),
    "interconnector": ("DISPATCHINTERCONNECTORRES", "interconnector_id"),
}


def staged_rows(file_table: str) -> int:
    total = 0
    for p in Path("data/backfill/staged/2022-06", file_table).glob("*.jsonl"):
        with open(p, encoding="utf-8") as f:
            total += sum(1 for _ in f)
    return total


spark = get_spark("reconcile_backfill")
print(f"{'table':15}{'staged':>9}{'bronze':>9}{'b_unique':>10}{'silver':>9}{'s_dupes':>9}{'quarant.':>10}")
for t, (file_table, ent) in TABLES.items():
    r = spark.sql(f"""
        SELECT
          (SELECT count(*) FROM local.bronze.{t}
            WHERE raw_value LIKE '%mmsdm_backfill%') AS bronze,
          (SELECT count(DISTINCT msg_key, get_json_object(raw_value, '$.data.INTERVENTION'))
             FROM local.bronze.{t} WHERE raw_value LIKE '%mmsdm_backfill%') AS b_unique,
          (SELECT count(*) FROM local.silver.{t}
            WHERE settlement_nem BETWEEN '{LO}' AND '{HI}') AS silver,
          (SELECT count(*) FROM (
             SELECT 1 FROM local.silver.{t}
             WHERE settlement_nem BETWEEN '{LO}' AND '{HI}'
             GROUP BY {ent}, settlement_utc, intervention HAVING count(*) > 1)) AS s_dupes,
          (SELECT count(*) FROM local.silver.{t}_quarantine
            WHERE raw_value LIKE '%mmsdm_backfill%') AS quarantined
    """).first()
    print(f"{t:15}{staged_rows(file_table):9d}{r.bronze:9d}{r.b_unique:10d}"
          f"{r.silver:9d}{r.s_dupes:9d}{r.quarantined:10d}")
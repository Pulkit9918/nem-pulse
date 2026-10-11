"""Backfill months one at a time: stage -> replay -> reconcile -> clean up."""

import argparse
import calendar
import csv
import shutil
import subprocess
import sys
import time
from pathlib import Path

from backfill.stage import STAGED
from backfill.stage import main as stage_month
from spark_jobs.common import get_spark

LOG = Path("data/backfill/reconcile_log.csv")
MIN_FREE_GB = 5.0
# MMSDM table -> (silver table, silver entity column, entity field in the JSON payload)
SILVER = {
    "DISPATCHPRICE": ("price", "region_id", "REGIONID"),
    "DISPATCHREGIONSUM": ("regionsum", "region_id", "REGIONID"),
    "DISPATCHINTERCONNECTORRES": (
        "interconnector", "interconnector_id", "INTERCONNECTORID"),
}
FIELDS = ["month", "ok", "minutes"] + [
    f"{t}_{k}" for t, _, _ in SILVER.values()
    for k in ("staged", "silver", "quarantined", "dupes")
]


def month_bounds(year: int, month: int) -> tuple[int, str, str]:
    last_day = calendar.monthrange(year, month)[1]
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    return last_day, f"{year}/{month:02d}/01 00:05:00", f"{ny}/{nm:02d}/01 00:00:00"


def months(start: str, end: str):
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    while (y, m) <= (ey, em):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def staged_counts(year: int, month: int) -> dict[str, int]:
    out = {}
    for file_table in SILVER:
        total = 0
        for p in (STAGED / f"{year}-{month:02d}" / file_table).glob("*.jsonl"):
            with open(p, encoding="utf-8") as f:
                total += sum(1 for _ in f)
        out[file_table] = total
    return out


def silver_counts(spark, lo: str, hi: str) -> dict[str, tuple[int, int, int]]:
    out = {}
    for file_table, (t, ent, json_ent) in SILVER.items():
        r = spark.sql(f"""
            SELECT
              (SELECT count(*) FROM local.silver.{t}
                WHERE settlement_nem BETWEEN '{lo}' AND '{hi}') AS silver,
              (SELECT count(*) FROM (
                 SELECT 1 FROM local.silver.{t}
                 WHERE settlement_nem BETWEEN '{lo}' AND '{hi}'
                 GROUP BY {ent}, settlement_utc, intervention HAVING count(*) > 1)) AS dupes,
              (SELECT count(DISTINCT get_json_object(raw_value, '$.settlement_nem'),
                                     get_json_object(raw_value, '$.data.{json_ent}'),
                                     get_json_object(raw_value, '$.data.INTERVENTION'))
                 FROM local.silver.{t}_quarantine
                WHERE raw_value LIKE '%mmsdm_backfill%'
                  AND get_json_object(raw_value, '$.settlement_nem')
                      BETWEEN '{lo}' AND '{hi}') AS quarantined
        """).first()
        out[file_table] = (r.silver, r.dupes, r.quarantined)
    return out


def wait_for_reconcile(spark, lo, hi, expected, timeout_min):
    deadline = time.monotonic() + timeout_min * 60
    while True:
        got = silver_counts(spark, lo, hi)
        ok = all(
            got[t][0] + got[t][2] == expected[t] and got[t][1] == 0 for t in expected
        )
        if ok or time.monotonic() > deadline:
            return ok, got
        time.sleep(30)


def done_months() -> set[str]:
    if not LOG.exists():
        return set()
    with open(LOG, encoding="utf-8") as f:
        return {row["month"] for row in csv.DictReader(f) if row["ok"] == "True"}


def append_log(row: dict) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    is_new = not LOG.exists()
    with open(LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if is_new:
            w.writeheader()
        w.writerow(row)


def run_month(spark, year: int, month: int, speed: float, timeout_min: float) -> bool:
    label = f"{year}-{month:02d}"
    last_day, lo, hi = month_bounds(year, month)
    started = time.monotonic()

    stage_month(year, month)
    expected = staged_counts(year, month)
    subprocess.run(
        [sys.executable, "-m", "backfill.replay",
         "--start", f"{label}-01", "--end", f"{label}-{last_day:02d}",
         "--speed", str(speed)],
        check=True,
    )
    ok, got = wait_for_reconcile(spark, lo, hi, expected, timeout_min)

    minutes = (time.monotonic() - started) / 60
    row = {"month": label, "ok": ok, "minutes": f"{minutes:.1f}"}
    for file_table, (t, _, _) in SILVER.items():
        silver, dupes, quarantined = got[file_table]
        row.update({f"{t}_staged": expected[file_table], f"{t}_silver": silver,
                    f"{t}_quarantined": quarantined, f"{t}_dupes": dupes})
    append_log(row)
    print(f"\n{label}: {'RECONCILED' if ok else 'MISMATCH'} in {minutes:.1f} min")
    for file_table, (t, _, _) in SILVER.items():
        silver, dupes, quarantined = got[file_table]
        print(f"  {t:15} staged {expected[file_table]:7d}  silver {silver:7d}  "
              f"quarantined {quarantined:4d}  dupes {dupes}")
    if ok:
        shutil.rmtree(STAGED / label, ignore_errors=True)
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True, help="first month, YYYY-MM")
    ap.add_argument("--end", required=True, help="last month, YYYY-MM")
    ap.add_argument("--speed", type=float, default=3000.0)
    ap.add_argument("--timeout", type=float, default=45.0,
                    help="minutes to wait for silver to catch up, per month")
    args = ap.parse_args()

    spark = get_spark("run_months")
    finished = done_months()
    for year, month in months(args.start, args.end):
        label = f"{year}-{month:02d}"
        if label in finished:
            print(f"{label}: already reconciled, skipping")
            continue
        free_gb = shutil.disk_usage(".").free / 1e9
        if free_gb < MIN_FREE_GB:
            print(f"stopping: only {free_gb:.1f} GB free (minimum {MIN_FREE_GB})")
            return 1
        if not run_month(spark, year, month, args.speed, args.timeout):
            print(f"{label}: did NOT reconcile; staged files kept; stopping")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
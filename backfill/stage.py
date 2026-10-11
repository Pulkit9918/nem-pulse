import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from backfill.probe import fetch, open_csv
from ingest.parser import nem_to_utc_iso, parse_mms_csv

STAGED = Path("data/backfill/staged")
FMT = "%Y/%m/%d %H:%M:%S"
# MMSDM file table -> (parser key, entity column, Kafka topic); same topics as the live poller
TABLES = {
    "DISPATCHPRICE": ("DISPATCH.PRICE", "REGIONID", "nem.dispatch.price"),
    "DISPATCHREGIONSUM": ("DISPATCH.REGIONSUM", "REGIONID", "nem.dispatch.regionsum"),
    "DISPATCHINTERCONNECTORRES": (
        "DISPATCH.INTERCONNECTORRES", "INTERCONNECTORID", "nem.dispatch.interconnector"),
}


def day_of(ts: str) -> str:
    """NEM calendar day of a period-ending stamp (00:00:00 belongs to the day before)."""
    return (datetime.strptime(ts, FMT) - timedelta(minutes=5)).strftime("%Y-%m-%d")


def stage_table(file_table: str, year: int, month: int) -> Counter:
    key, entity_col, topic = TABLES[file_table]
    path = fetch(file_table, year, month)
    out_dir = STAGED / f"{year}-{month:02d}" / file_table
    out_dir.mkdir(parents=True, exist_ok=True)
    handles, counts = {}, Counter()
    _, fh = open_csv(path)
    try:
        for k, rec in parse_mms_csv(fh):
            if k != key:
                continue
            ts = rec["SETTLEMENTDATE"]
            day = day_of(ts)
            if day not in handles:
                handles[day] = open(out_dir / f"{day}.jsonl", "w", encoding="utf-8")
            msg = {
                "table": key,
                "origin": "mmsdm_backfill",
                "source_file": path.name,
                "settlement_nem": ts,
                "settlement_utc": nem_to_utc_iso(ts),
                "topic": topic,
                "msg_key": f"{rec[entity_col]}|{ts}",
                "data": rec,
            }
            handles[day].write(json.dumps(msg) + "\n")
            counts[day] += 1
    finally:
        for h in handles.values():
            h.close()
    return counts


def main(year: int = 2022, month: int = 6) -> None:
    print(f"staging {year}-{month:02d} into {STAGED}")
    for table in TABLES:
        counts = stage_table(table, year, month)
        print(f"  {table}: {sum(counts.values())} rows over {len(counts)} days "
              f"(per day: min {min(counts.values())}, max {max(counts.values())})")


if __name__ == "__main__":
    main(*[int(a) for a in sys.argv[1:3]])
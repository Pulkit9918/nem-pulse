"""Build reference/price_regimes.csv from AEMO's notices and administered-pricing events."""

import csv
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

YEAR, MONTH = 2022, 6
FMT = "%Y/%m/%d %H:%M:%S"
STEP = timedelta(minutes=5)
REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
OUT = Path("reference/price_regimes.csv")

# Period-ending NEM stamps, inclusive at both ends, taken from AEMO's market notices.
SUSPENSION = [
    ("suspended_schedule", "2022/06/15 14:05:00", "2022/06/23 04:00:00",
     "AEMO notices: declaration 15 Jun 14:03 (schedule pricing from TI 1405); "
     "change of pricing regime from TI 0405 on 23 Jun"),
    ("suspended_dispatch", "2022/06/23 04:05:00", "2022/06/24 14:00:00",
     "AEMO notices: dispatch pricing from TI 0405 on 23 Jun; resumption at 1400 hrs on 24 Jun"),
]


def parse(ts: str) -> datetime:
    return datetime.strptime(ts, FMT)


def stamps(start: str, end: str) -> set[datetime]:
    t, last, out = parse(start), parse(end), set()
    while t <= last:
        out.add(t)
        t += STEP
    return out


def administered_windows() -> list[tuple]:
    events = {}
    _, fh = open_csv(fetch("APEVENT", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        events[rec["APEVENTID"]] = (rec["EFFECTIVEFROMINTERVAL"], rec["EFFECTIVETOINTERVAL"])
    rows = []
    _, fh = open_csv(fetch("APEVENTREGION", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        if rec.get("ENERGYAPFLAG") == "1" and rec["APEVENTID"] in events:
            start, end = events[rec["APEVENTID"]]
            rows.append((rec["REGIONID"], "administered", start, end,
                         f"APEVENT {rec['APEVENTID']}"))
    return rows


def build_rows() -> list[tuple]:
    rows = administered_windows()
    for regime, start, end, src in SUSPENSION:
        rows += [(region, regime, start, end, src) for region in REGIONS]
    return sorted(rows)


def write(rows: list[tuple]) -> None:
    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["region_id", "regime", "from_nem", "to_nem", "source"])
        w.writerows(rows)


def validate(rows: list[tuple]) -> None:
    flagged, iv, prices = defaultdict(set), set(), []
    _, fh = open_csv(fetch("DISPATCHPRICE", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        t = parse(rec["SETTLEMENTDATE"])
        if rec["INTERVENTION"] == "1":
            iv.add(t)
            continue
        if rec.get("MARKETSUSPENDEDFLAG") == "1":
            flagged[rec["REGIONID"]].add(t)
        if rec.get("RRP"):
            prices.append((rec["REGIONID"], t, float(rec["RRP"])))

    print("\n1. MARKETSUSPENDEDFLAG vs the dispatch-pricing window from the notices")
    expected = stamps(*SUSPENSION[1][1:3])
    for region in REGIONS:
        got = flagged[region]
        print(f"  {region}: flagged {len(got)}, window {len(expected)}, "
              f"missing {len(expected - got)}, extra {len(got - expected)}")

    print("\n2. Intervention rows vs the schedule-pricing window")
    window = stamps(*SUSPENSION[0][1:3])
    print(f"  intervals with intervention rows: {len(iv)}")
    print(f"  schedule window: {len(window)} intervals, {len(window & iv)} have intervention rows")
    print(f"  intervention intervals outside the window: {len(iv - window)}")

    print("\n3. Energy prices inside administered windows (expect -300..300)")
    by_region = defaultdict(list)
    for region, regime, start, end, _ in rows:
        if regime == "administered":
            by_region[region].append((parse(start), parse(end)))
    for region in REGIONS:
        inside = [(t, p) for r, t, p in prices
                  if r == region and any(a <= t <= b for a, b in by_region[region])]
        bad = [(t, p) for t, p in inside if p < -300 or p > 300]
        print(f"  {region}: {len(inside)} intervals in administered windows, "
              f"{len(bad)} outside -300..300")
        for t, p in sorted(bad)[:4]:
            print(f"      {t:%m-%d %H:%M}  {p:.2f}")


def main() -> None:
    rows = build_rows()
    write(rows)
    counts = defaultdict(int)
    for r in rows:
        counts[r[1]] += 1
    print(f"wrote {len(rows)} rows to {OUT}: {dict(counts)}")
    validate(rows)


if __name__ == "__main__":
    main()
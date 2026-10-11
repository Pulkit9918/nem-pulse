"""Administered-pricing events since Aug 2024, and what prices did inside them."""

from collections import defaultdict
from datetime import datetime

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

FMT = "%Y/%m/%d %H:%M:%S"


def events_since(start: str) -> dict:
    _, fh = open_csv(fetch("APEVENT", 2026, 8))
    events = {}
    for _, rec in parse_mms_csv(fh):
        if rec["EFFECTIVEFROMINTERVAL"] >= start:
            events[rec["APEVENTID"]] = rec
    _, fh = open_csv(fetch("APEVENTREGION", 2026, 8))
    for _, rec in parse_mms_csv(fh):
        ev = events.get(rec["APEVENTID"])
        if ev is not None and rec.get("ENERGYAPFLAG") == "1":
            ev.setdefault("regions", []).append(rec["REGIONID"])
    return events


def prices(year: int, month: int) -> dict:
    out = defaultdict(list)
    _, fh = open_csv(fetch("DISPATCHPRICE", year, month))
    for _, rec in parse_mms_csv(fh):
        if rec.get("INTERVENTION") == "0" and rec.get("RRP"):
            t = datetime.strptime(rec["SETTLEMENTDATE"], FMT)
            out[rec["REGIONID"]].append((t, float(rec["RRP"])))
    return out


def main() -> None:
    events = events_since("2024/08")
    print(f"{len(events)} administered-pricing events starting since 2024/08\n")
    ordered = sorted(events.items(), key=lambda kv: kv[1]["EFFECTIVEFROMINTERVAL"])
    for eid, ev in ordered:
        print(f"  {eid}: {ev['EFFECTIVEFROMINTERVAL']} -> {ev['EFFECTIVETOINTERVAL']} "
              f"regions {ev.get('regions')}")
        print(f"      {ev['REASON'][:170]}")

    print("\nEnergy prices inside the August 2025 events (INTERVENTION=0):")
    aug = prices(2025, 8)
    for eid, ev in ordered:
        if not ev["EFFECTIVEFROMINTERVAL"].startswith("2025/08"):
            continue
        t0 = datetime.strptime(ev["EFFECTIVEFROMINTERVAL"], FMT)
        t1 = datetime.strptime(ev["EFFECTIVETOINTERVAL"], FMT)
        for region in ev.get("regions", []):
            inside = [p for t, p in aug[region] if t0 <= t <= t1]
            if inside:
                print(f"  event {eid} {region}: {len(inside)} intervals, "
                      f"min {min(inside):.1f}, max {max(inside):.1f}")

    print("\nREGIONAPC (administered price cap values), latest rows:")
    _, fh = open_csv(fetch("REGIONAPC", 2026, 8))
    rows = sorted((rec for _, rec in parse_mms_csv(fh)), key=lambda r: r.get("EFFECTIVEDATE", ""))
    for rec in rows[-12:]:
        print(" ", rec)


if __name__ == "__main__":
    main()
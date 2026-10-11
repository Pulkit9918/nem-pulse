from collections import Counter, defaultdict
from datetime import datetime, timedelta

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

YEAR, MONTH = 2022, 6
FMT = "%Y/%m/%d %H:%M:%S"
SILVER_NUMERIC = [
    "TOTALDEMAND", "AVAILABLEGENERATION", "DISPATCHABLEGENERATION", "NETINTERCHANGE",
    "CLEAREDSUPPLY", "DEMANDFORECAST", "DEMAND_AND_NONSCHEDGEN",
    "TOTALINTERMITTENTGENERATION", "SS_SOLAR_CLEAREDMW", "SS_SOLAR_AVAILABILITY",
    "SS_WIND_CLEAREDMW", "SS_WIND_AVAILABILITY", "BDU_CLEAREDMW_GEN",
    "BDU_CLEAREDMW_LOAD", "LORSURPLUS", "LRCSURPLUS",
]


def day_of(ts: str) -> str:
    """Period-ending stamps: 00:00:00 belongs to the previous calendar day."""
    return (datetime.strptime(ts, FMT) - timedelta(minutes=5)).strftime("%Y-%m-%d")


def a_price_by_day() -> None:
    print("\n=== A. Pricing by day (INTERVENTION=0 rows, except iv_rows)")
    print("day         rows0  iv_rows  suspended  apc_flag   min_rrp   max_rrp  mean_rrp")
    days = defaultdict(lambda: {"rows0": 0, "iv": 0, "susp": 0, "apc": 0,
                                "mn": float("inf"), "mx": float("-inf"), "sum": 0.0})
    _, fh = open_csv(fetch("DISPATCHPRICE", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        d = days[day_of(rec["SETTLEMENTDATE"])]
        if rec["INTERVENTION"] == "1":
            d["iv"] += 1
            continue
        if not rec.get("RRP"):
            continue
        rrp = float(rec["RRP"])
        d["rows0"] += 1
        d["sum"] += rrp
        d["mn"] = min(d["mn"], rrp)
        d["mx"] = max(d["mx"], rrp)
        if rec.get("MARKETSUSPENDEDFLAG") == "1":
            d["susp"] += 1
        if rec.get("APCFLAG", "0") not in ("0", ""):
            d["apc"] += 1
    for day in sorted(days):
        d = days[day]
        mean = d["sum"] / max(d["rows0"], 1)
        print(f"{day}  {d['rows0']:5d}  {d['iv']:7d}  {d['susp']:9d}  {d['apc']:8d}  "
              f"{d['mn']:8.1f}  {d['mx']:8.1f}  {mean:8.1f}")


def b_regionsum_population() -> None:
    print("\n=== B. Share of 2022 regionsum rows that populate each silver column")
    total, filled = 0, Counter()
    _, fh = open_csv(fetch("DISPATCHREGIONSUM", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        if rec["INTERVENTION"] != "0":
            continue
        total += 1
        for c in SILVER_NUMERIC:
            if rec.get(c) not in (None, ""):
                filled[c] += 1
    for c in SILVER_NUMERIC:
        print(f"  {c:30} {100 * filled[c] / total:6.1f}% populated")


def c_which_run_settles() -> None:
    print("\n=== C. Which INTERVENTION value reproduces AEMO's 30-minute settlement price?")
    steps = defaultdict(dict)  # (region, trading end) -> {dispatch time: {flag: rrp}}
    _, fh = open_csv(fetch("DISPATCHPRICE", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        if not rec.get("RRP"):
            continue
        t = datetime.strptime(rec["SETTLEMENTDATE"], FMT)
        end = t if t.minute % 30 == 0 else t + timedelta(minutes=30 - t.minute % 30)
        steps[(rec["REGIONID"], end)].setdefault(t, {})[rec["INTERVENTION"]] = float(rec["RRP"])

    _, fh = open_csv(fetch("TRADINGPRICE", YEAR, MONTH))
    trading, shown = {}, 0
    for _, rec in parse_mms_csv(fh):
        if not rec.get("RRP") or "REGIONID" not in rec:
            continue
        if shown < 2:
            print("  sample trading row:", {k: rec[k] for k in list(rec)[:8]})
            shown += 1
        key = (rec["REGIONID"], datetime.strptime(rec["SETTLEMENTDATE"], FMT))
        trading[key] = float(rec["RRP"])

    stats = Counter()
    for key, tp in trading.items():
        s = steps.get(key)
        if not s or len(s) != 6 or any("0" not in v for v in s.values()):
            stats["skipped (incomplete window)"] += 1
            continue
        avg0 = sum(v["0"] for v in s.values()) / 6
        alt = sum(v.get("1", v["0"]) for v in s.values()) / 6
        kind = "has intervention rows" if any("1" in v for v in s.values()) else "no intervention rows"
        stats[(kind,
               f"INTERVENTION=0 matches: {abs(avg0 - tp) < 0.01}",
               f"INTERVENTION=1 (where present) matches: {abs(alt - tp) < 0.01}")] += 1
    for k, n in sorted(stats.items(), key=lambda kv: str(kv[0])):
        print(f"  {n:6d}  {k}")


def d_admin_pricing_tables() -> None:
    print("\n=== D. Administered-pricing tables (rows mentioning 2022/06 or 2022/07)")
    for table in ("APEVENT", "APEVENTREGION", "REGIONAPCINTERVALS", "OVERRIDERRP"):
        _, fh = open_csv(fetch(table, YEAR, MONTH))
        hits, total = [], 0
        for _, rec in parse_mms_csv(fh):
            total += 1
            if any(v.startswith(("2022/06", "2022/07")) for v in rec.values()):
                hits.append(rec)
        print(f"\n  {table}: {total} rows in table, {len(hits)} mention June/July 2022")
        for rec in hits[:15]:
            print("   ", rec)


def main() -> None:
    a_price_by_day()
    b_regionsum_population()
    c_which_run_settles()
    d_admin_pricing_tables()


if __name__ == "__main__":
    main()
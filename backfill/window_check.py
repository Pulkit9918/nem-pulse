"""Profile each candidate month cheaply: price table + administered-pricing events."""

import requests

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

MONTHS = [(2024, m) for m in range(7, 13)] + [(2025, m) for m in range(1, 13)] + [
    (2026, m) for m in range(1, 9)
]


def profile(year: int, month: int) -> str:
    _, fh = open_csv(fetch("DISPATCHPRICE", year, month))
    n = neg = over300 = over1000 = iv = susp = 0
    top = float("-inf")
    for _, rec in parse_mms_csv(fh):
        if rec.get("INTERVENTION") == "1":
            iv += 1
            continue
        if not rec.get("RRP"):
            continue
        rrp = float(rec["RRP"])
        n += 1
        neg += rrp < 0
        over300 += rrp > 300
        over1000 += rrp > 1000
        susp += rec.get("MARKETSUSPENDEDFLAG") == "1"
        top = max(top, rrp)

    _, fh = open_csv(fetch("APEVENT", year, month))
    total = in_month = 0
    for _, rec in parse_mms_csv(fh):
        total += 1
        if rec.get("EFFECTIVEFROMINTERVAL", "").startswith(f"{year}/{month:02d}"):
            in_month += 1

    d = max(n, 1)
    return (
        f"{year}-{month:02d} | rows {n:6d} | interv {iv:5d} | susp {susp:5d} | "
        f"apevent {in_month:3d}/{total:<3d} | neg {100 * neg / d:5.1f}% | "
        f">300 {100 * over300 / d:5.2f}% | >1000 {100 * over1000 / d:5.2f}% | max {top:8.1f}"
    )


def thresholds() -> None:
    print("\nMarket price thresholds effective since mid-2024:")
    _, fh = open_csv(fetch("MARKET_PRICE_THRESHOLDS", 2026, 8))
    rows = [rec for _, rec in parse_mms_csv(fh) if rec["EFFECTIVEDATE"] >= "2024/07/01"]
    for rec in sorted(rows, key=lambda r: r["EFFECTIVEDATE"]):
        print(f"  effective {rec['EFFECTIVEDATE'][:10]}: "
              f"VOLL {rec['VOLL']}, floor {rec['MARKETPRICEFLOOR']}")


def main() -> None:
    for year, month in MONTHS:
        try:
            print(profile(year, month))
        except requests.HTTPError as e:
            print(f"{year}-{month:02d}: not available (HTTP {e.response.status_code})")
    thresholds()


if __name__ == "__main__":
    main()
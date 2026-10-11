from collections import Counter, defaultdict

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

YEAR, MONTH = 2022, 6


def trading_vs_dispatch() -> None:
    print("\n=== 1. TRADINGPRICE vs DISPATCHPRICE at the same region and timestamp")
    disp = defaultdict(dict)
    _, fh = open_csv(fetch("DISPATCHPRICE", YEAR, MONTH))
    for _, rec in parse_mms_csv(fh):
        if rec.get("RRP"):
            disp[(rec["REGIONID"], rec["SETTLEMENTDATE"])][rec["INTERVENTION"]] = float(rec["RRP"])

    _, fh = open_csv(fetch("TRADINGPRICE", YEAR, MONTH))
    stats, total = Counter(), 0
    for _, rec in parse_mms_csv(fh):
        if not rec.get("RRP") or "REGIONID" not in rec:
            continue
        total += 1
        d = disp.get((rec["REGIONID"], rec["SETTLEMENTDATE"]))
        if d is None:
            stats["no dispatch row at that timestamp"] += 1
            continue
        tp = float(rec["RRP"])
        kind = "both runs published" if len(d) == 2 else "single run only"
        m0 = abs(d["0"] - tp) < 0.01 if "0" in d else None
        m1 = abs(d["1"] - tp) < 0.01 if "1" in d else None
        stats[(kind, f"INTERVENTION=0 matches: {m0}", f"INTERVENTION=1 matches: {m1}")] += 1
    print(f"  TRADINGPRICE rows: {total}")
    for k, n in sorted(stats.items(), key=lambda kv: str(kv[0])):
        print(f"  {n:6d}  {k}")


def thresholds() -> None:
    print("\n=== 2. Market price thresholds")
    _, fh = open_csv(fetch("MARKET_PRICE_THRESHOLDS", YEAR, MONTH))
    for key, rec in parse_mms_csv(fh):
        print(" ", key, rec)


def notices() -> None:
    print("\n=== 3. Market notices mentioning a suspension, dated June 2022")
    _, fh = open_csv(fetch("MARKETNOTICEDATA", YEAR, MONTH))
    hits = total = 0
    for _, rec in parse_mms_csv(fh):
        total += 1
        text = " ".join(rec.values())
        if "suspen" in text.lower() and any(v.startswith("2022/06") for v in rec.values()):
            hits += 1
            if hits <= 12:
                print(f"  [{rec.get('EFFECTIVEDATE', '?')}] {rec.get('REASON', text)[:260]!r}")
    print(f"  {hits} matching notices out of {total} total")


if __name__ == "__main__":
    trading_vs_dispatch()
    thresholds()
    notices()
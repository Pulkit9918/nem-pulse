import io
import zipfile
from collections import Counter
from pathlib import Path
from urllib.parse import quote

import requests

from ingest.parser import parse_mms_csv

BASE = (
    "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
    "{y}/MMSDM_{y}_{m}/MMSDM_Historical_Data_SQLLoader/DATA/"
)
RAW = Path("data/backfill/raw")
HEADERS = {"User-Agent": "nem-pulse/0.1 (personal portfolio project)"}


def candidate_names(table: str, year: int, month: int) -> list[str]:
    stamp = f"{year}{month:02d}010000"
    return [
        f"PUBLIC_DVD_{table}_{stamp}.zip", 
        f"PUBLIC_ARCHIVE#{table}#FILE01#{stamp}.zip",  
    ]


def fetch(table: str, year: int, month: int) -> Path:
    RAW.mkdir(parents=True, exist_ok=True)
    names = candidate_names(table, year, month)
    for name in names:
        if (RAW / name).exists():
            return RAW / name

    base = BASE.format(y=year, m=f"{month:02d}")
    response = None
    for name in names:
        response = requests.get(
            base + quote(name, safe=""), headers=HEADERS, stream=True, timeout=120
        )
        if response.status_code == 404:
            response.close()
            continue
        response.raise_for_status()
        path = RAW / name
        part = path.with_name(path.name + ".part")
        with response, open(part, "wb") as f:
            for chunk in response.iter_content(1 << 20):
                f.write(chunk)
        part.replace(path)  
        return path
    raise requests.HTTPError(
        f"{table} {year}-{month:02d}: not found as {names}", response=response
    )


def open_csv(path: Path):
    z = zipfile.ZipFile(path)
    inner = z.namelist()[0]
    return inner, io.TextIOWrapper(z.open(inner), encoding="utf-8", errors="replace")


def probe(table: str, year: int, month: int, show_rows: int = 0, want: tuple = ()) -> None:
    path = fetch(table, year, month)
    inner, fh = open_csv(path)
    print(f"\n=== {table} {year}-{month:02d} | zip {path.stat().st_size / 1e6:.1f} MB | inner file {inner}")
    for i, line in enumerate(fh):
        print("  raw:", line.rstrip()[:170])
        if i == 2:
            break
    fh.close()

    _, fh = open_csv(path)
    tables, cols = Counter(), {}
    entities, intervention = Counter(), Counter()
    lo = hi = None
    shown = []
    for key, rec in parse_mms_csv(fh):
        tables[key] += 1
        cols.setdefault(key, list(rec))
        ts = rec.get("SETTLEMENTDATE")
        if ts:
            lo = ts if lo is None or ts < lo else lo
            hi = ts if hi is None or ts > hi else hi
        ent = rec.get("REGIONID") or rec.get("INTERCONNECTORID")
        if ent:
            entities[ent] += 1
        if "INTERVENTION" in rec:
            intervention[rec["INTERVENTION"]] += 1
        if len(shown) < show_rows:
            shown.append(rec)

    for key, n in tables.items():
        print(f"  table {key}: {n} rows, {len(cols[key])} columns")
        print(f"  first columns: {cols[key][:12]}")
        if want:
            print(f"  wanted columns present: { {c: c in cols[key] for c in want} }")
    print(f"  settlement range: {lo} -> {hi}")
    if entities:
        print(f"  rows per region/interconnector: {dict(entities)}")
    if intervention:
        print(f"  INTERVENTION values: {dict(intervention)}")
    for rec in shown:
        print("  row:", rec)


def main() -> None:
    y, m = 2022, 6
    probe("DISPATCHPRICE", y, m)
    probe("DISPATCHREGIONSUM", y, m,
          want=("TOTALDEMAND", "NETINTERCHANGE", "SS_SOLAR_CLEAREDMW",
                "SS_WIND_CLEAREDMW", "BDU_CLEAREDMW_GEN", "LORSURPLUS"))
    probe("DISPATCHINTERCONNECTORRES", y, m)
    probe("MARKETSUSPENSION", y, m, show_rows=20)
    probe("MARKETSUSREGION", y, m, show_rows=20)
    probe("ROOFTOP_PV_ACTUAL", y, m, show_rows=3)


if __name__ == "__main__":
    main()
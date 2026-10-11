"""Check that each month has all three tables, without downloading anything."""

import sys
from urllib.parse import quote

import requests

from backfill.probe import BASE, HEADERS, candidate_names

TABLES = ["DISPATCHPRICE", "DISPATCHREGIONSUM", "DISPATCHINTERCONNECTORRES"]


def months(start: str, end: str):
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    while (y, m) <= (ey, em):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def remote_size(table: str, year: int, month: int) -> tuple[int | None, str]:
    base = BASE.format(y=year, m=f"{month:02d}")
    for name in candidate_names(table, year, month):
        url = base + quote(name, safe="")
        r = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code == 405:  # server refuses HEAD; fall back to a streamed GET
            r = requests.get(url, headers=HEADERS, stream=True, timeout=30)
            r.close()
        if r.status_code == 200:
            return int(r.headers.get("Content-Length", 0)), name
    return None, ""


def main() -> int:
    start, end = sys.argv[1:3] if len(sys.argv) >= 3 else ("2024-08", "2026-08")
    missing = 0
    for year, month in months(start, end):
        cells = []
        for table in TABLES:
            size, name = remote_size(table, year, month)
            if size is None:
                cells.append("MISSING ")
                missing += 1
            else:
                cells.append(f"{size / 1e6:5.1f} MB{'*' if '#' in name else ' '}")
        print(f"{year}-{month:02d} | " + " | ".join(cells))
    print("\n* = newer PUBLIC_ARCHIVE file naming")
    print(f"{missing} missing files")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
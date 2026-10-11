"""Which months can feed the model? Sample the monthly MMSDM archive."""

import requests

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv

MONTHS = [(2024, 7), (2024, 10), (2025, 1), (2025, 7), (2026, 1), (2026, 4), (2026, 8)]
CHECK = ["SS_SOLAR_AVAILABILITY", "SS_WIND_AVAILABILITY", "BDU_CLEAREDMW_GEN", "LORSURPLUS"]


def regionsum_population(year: int, month: int) -> tuple[int, dict]:
    _, fh = open_csv(fetch("DISPATCHREGIONSUM", year, month))
    total, filled = 0, dict.fromkeys(CHECK, 0)
    for _, rec in parse_mms_csv(fh):
        if rec.get("INTERVENTION") != "0":
            continue
        total += 1
        for c in CHECK:
            if rec.get(c) not in (None, ""):
                filled[c] += 1
    return total, filled


def interconnectors(year: int, month: int) -> list[str]:
    _, fh = open_csv(fetch("DISPATCHINTERCONNECTORRES", year, month))
    return sorted({rec["INTERCONNECTORID"] for _, rec in parse_mms_csv(fh)})


def main() -> None:
    for year, month in MONTHS:
        label = f"{year}-{month:02d}"
        try:
            total, filled = regionsum_population(year, month)
            ics = interconnectors(year, month)
        except requests.HTTPError as e:
            print(f"{label}: not available (HTTP {e.response.status_code})")
            continue
        pct = ", ".join(f"{c}={100 * filled[c] / max(total, 1):.0f}%" for c in CHECK)
        print(f"{label}: {total} rows | {pct} | {len(ics)} interconnectors {ics}")


if __name__ == "__main__":
    main()
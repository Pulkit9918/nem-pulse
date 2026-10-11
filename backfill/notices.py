import base64

from backfill.probe import fetch, open_csv
from ingest.parser import parse_mms_csv


def main() -> None:
    _, fh = open_csv(fetch("MARKETNOTICEDATA", 2022, 6))
    rows = []
    for _, rec in parse_mms_csv(fh):
        when = rec.get("EFFECTIVEDATE", "")
        if not when.startswith("2022/06"):
            continue
        try:
            text = base64.b64decode(rec.get("REASON", "")).decode("utf-8", errors="replace")
        except ValueError:
            continue
        if "suspen" in text.lower() or "resumption" in text.lower():
            rows.append((when, " ".join(text.split())))
    for when, text in sorted(rows):
        print(f"\n[{when}]\n  {text[:700]}")


if __name__ == "__main__":
    main()
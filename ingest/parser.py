import csv
import io
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone

# NEM time is fixed AEST (UTC+10). It does NOT observe daylight saving.
NEM_TZ = timezone(timedelta(hours=10))


def parse_mms_csv(text: str) -> Iterator[tuple[str, dict]]:
    """Yield (table_key, record) for every D row, using the I-row headers.

    table_key looks like 'DISPATCH.PRICE'. Columns are read from the file
    itself, so AEMO adding columns or bumping a table version won't break us.
    """
    headers: dict[str, list[str]] = {}
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 5:
            continue
        kind = row[0]
        key = f"{row[1]}.{row[2]}"
        if kind == "I":
            headers[key] = row[4:]
        elif kind == "D":
            cols = headers.get(key)
            if cols is not None:
                yield key, dict(zip(cols, row[4:]))


def nem_to_utc_iso(ts: str) -> str:
    """'2026/10/11 12:25:00' (NEM time, period-ending) -> ISO-8601 UTC."""
    dt = datetime.strptime(ts, "%Y/%m/%d %H:%M:%S").replace(tzinfo=NEM_TZ)
    return dt.astimezone(UTC).isoformat()
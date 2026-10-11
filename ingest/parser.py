import csv
import io
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta, timezone

NEM_TZ = timezone(timedelta(hours=10))


def parse_mms_csv(source: str | Iterable[str]) -> Iterator[tuple[str, dict]]:
    """Yield (table_key, record) for every D row, using the I-row headers.

    `source` is either the whole file as a string or any iterable of lines
    (for example an open file), so large monthly archives can be streamed.
    table_key looks like 'DISPATCH.PRICE'. Columns are read from the file
    itself, so AEMO adding columns or bumping a table version won't break us.
    """
    lines = io.StringIO(source) if isinstance(source, str) else source
    headers: dict[str, list[str]] = {}
    for row in csv.reader(lines):
        if len(row) < 5:
            continue
        kind = row[0]
        key = f"{row[1]}.{row[2]}"
        if kind == "I":
            headers[key] = row[4:]
        elif kind == "D":
            cols = headers.get(key)
            if cols is not None:
                yield key, dict(zip(cols, row[4:], strict=False))


def nem_to_utc_iso(ts: str) -> str:
    """'2026/10/11 12:25:00' (NEM time, period-ending) -> ISO-8601 UTC."""
    dt = datetime.strptime(ts, "%Y/%m/%d %H:%M:%S").replace(tzinfo=NEM_TZ)
    return dt.astimezone(UTC).isoformat()
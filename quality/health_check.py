import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from quality import checks as c
from spark_jobs.common import get_spark

# table -> (entity column, expected distinct entities per interval)
# 7 interconnectors is what your data shows today; AEMO can add more over time.
TABLES = {
    "price": ("region_id", 5),
    "regionsum": ("region_id", 5),
    "interconnector": ("interconnector_id", 7),
}
WIN = "(SELECT max(settlement_utc) FROM local.silver.{t}) - INTERVAL 2 HOURS"


def utc(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def main() -> int:
    spark = get_spark("health_check")
    now = datetime.now(UTC)
    results = []

    for t, (key, expected) in TABLES.items():
        latest = spark.sql(
            f"SELECT cast(max(settlement_utc) AS bigint) AS ts FROM local.silver.{t}"
        ).first().ts
        results.append(c.check_freshness(f"freshness.{t}", utc(latest), now))

        rows = spark.sql(f"""
            SELECT cast(settlement_utc AS bigint) AS ts, count(DISTINCT {key}) AS n
            FROM local.silver.{t}
            WHERE settlement_utc >= {WIN.format(t=t)}
            GROUP BY settlement_utc
        """).collect()
        results.append(c.check_completeness(
            f"completeness.{t}", {utc(r.ts): r.n for r in rows}, expected))

        q = spark.sql(f"""
            SELECT
              (SELECT count(*) FROM local.silver.{t}
                WHERE ingested_at >= current_timestamp() - INTERVAL 24 HOURS) AS good,
              (SELECT count(*) FROM local.silver.{t}_quarantine
                WHERE quarantined_at >= current_timestamp() - INTERVAL 24 HOURS) AS bad
        """).first()
        results.append(c.check_quarantine_rate(f"quarantine_rate.{t}", q.good, q.bad))

    rows = spark.sql(f"""
        SELECT cast(settlement_utc AS bigint) AS ts, sum(netinterchange) AS s
        FROM local.silver.regionsum
        WHERE intervention = 0
          AND settlement_utc >= {WIN.format(t='regionsum')}
        GROUP BY settlement_utc
        HAVING count(DISTINCT region_id) = 5
    """).collect()
    results.append(c.check_net_interchange(
        "net_interchange.regionsum", {utc(r.ts): float(r.s) for r in rows}))

    for r in results:
        print(f"{r.status.upper():5} {r.name:32} {r.detail}")

    out = Path("data/health_report.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({
        "checked_at": now.isoformat(),
        "results": [r.__dict__ for r in results],
    }, indent=2))
    return c.exit_code(results)


if __name__ == "__main__":
    sys.exit(main())
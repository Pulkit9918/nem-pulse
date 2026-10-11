from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

STEP = timedelta(minutes=5)


@dataclass
class CheckResult:
    name: str
    status: str  # "pass" | "warn" | "fail"
    detail: str


def _fmt(t: datetime) -> str:
    return t.strftime("%m-%d %H:%MZ")


def check_freshness(name: str, latest: datetime, now: datetime,
                    max_age_min: float = 15.0) -> CheckResult:
    age = (now - latest).total_seconds() / 60
    if age < 0:
        detail = f"newest interval ends {-age:.1f} min ahead of now (normal: AEMO publishes ahead of the interval end)"
    else:
        detail = f"newest interval is {age:.1f} min old (limit {max_age_min:.0f})"
    return CheckResult(name, "pass" if age <= max_age_min else "fail", detail)


def check_completeness(name: str, counts: dict[datetime, int], expected: int) -> CheckResult:
    """counts: {interval_end_utc: distinct entities seen}. Walks the 5-minute grid."""
    if not counts:
        return CheckResult(name, "fail", "no rows in window")
    missing, short, extra = [], [], []
    t, last = min(counts), max(counts)
    while t <= last:
        n = counts.get(t, 0)
        if n == 0:
            missing.append(t)
        elif n < expected:
            short.append((t, n))
        elif n > expected:
            extra.append((t, n))
        t += STEP
    if missing or short:
        parts = []
        if missing:
            parts.append(f"{len(missing)} missing interval(s), first {_fmt(missing[0])}")
        if short:
            parts.append(f"{len(short)} interval(s) below {expected}, first {_fmt(short[0][0])} has {short[0][1]}")
        return CheckResult(name, "fail", "; ".join(parts))
    if extra:
        return CheckResult(name, "warn",
                           f"{len(extra)} interval(s) above {expected} (new entity?), first {_fmt(extra[0][0])} has {extra[0][1]}")
    return CheckResult(name, "pass", f"{len(counts)} intervals, all with {expected}")


def check_quarantine_rate(name: str, good: int, bad: int, max_rate: float = 0.01) -> CheckResult:
    total = good + bad
    if total == 0:
        return CheckResult(name, "warn", "no rows in the last 24h")
    rate = bad / total
    return CheckResult(name, "pass" if rate <= max_rate else "fail",
                       f"{bad}/{total} quarantined ({rate:.2%}, limit {max_rate:.0%})")


def check_net_interchange(name: str, sums: dict[datetime, float],
                          tolerance_mw: float = 250.0) -> CheckResult:
    """Net interchange summed over all regions should be near zero (losses only)."""
    if not sums:
        return CheckResult(name, "fail", "no complete intervals in window")
    worst_t = max(sums, key=lambda t: abs(sums[t]))
    worst = sums[worst_t]
    breaches = sum(1 for v in sums.values() if abs(v) > tolerance_mw)
    status = "pass" if breaches == 0 else "fail"
    return CheckResult(name, status,
                       f"worst |sum| = {abs(worst):.1f} MW at {_fmt(worst_t)} "
                       f"({breaches} interval(s) over {tolerance_mw:.0f} MW)")


def exit_code(results: list[CheckResult]) -> int:
    return 1 if any(r.status == "fail" for r in results) else 0
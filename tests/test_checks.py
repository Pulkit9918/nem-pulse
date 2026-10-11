from datetime import UTC, datetime, timedelta

from quality.checks import (
    check_completeness,
    check_freshness,
    check_net_interchange,
    check_quarantine_rate,
    exit_code,
)

T0 = datetime(2026, 10, 11, 5, 0, tzinfo=UTC)


def grid(n, count=5):
    return {T0 + timedelta(minutes=5 * i): count for i in range(n)}


def test_freshness_pass_and_fail():
    assert check_freshness("f", T0, T0 + timedelta(minutes=4)).status == "pass"
    assert check_freshness("f", T0, T0 + timedelta(minutes=30)).status == "fail"


def test_freshness_future_interval_is_normal():
    r = check_freshness("f", T0 + timedelta(minutes=4), T0)
    assert r.status == "pass"
    assert "ahead" in r.detail


def test_completeness_clean():
    assert check_completeness("c", grid(24), 5).status == "pass"


def test_completeness_detects_gap():
    counts = grid(24)
    del counts[T0 + timedelta(minutes=50)]
    r = check_completeness("c", counts, 5)
    assert r.status == "fail" and "missing" in r.detail


def test_completeness_detects_short_interval():
    counts = grid(24)
    counts[T0 + timedelta(minutes=20)] = 4
    assert check_completeness("c", counts, 5).status == "fail"


def test_completeness_extra_entity_is_warning_only():
    counts = grid(24)
    counts[T0 + timedelta(minutes=20)] = 6
    assert check_completeness("c", counts, 5).status == "warn"


def test_completeness_empty_fails():
    assert check_completeness("c", {}, 5).status == "fail"


def test_quarantine_rate():
    assert check_quarantine_rate("q", 3000, 3).status == "pass"
    assert check_quarantine_rate("q", 100, 50).status == "fail"
    assert check_quarantine_rate("q", 0, 0).status == "warn"


def test_net_interchange_real_values_pass():
    sums = {T0: 33.41, T0 + timedelta(minutes=5): 45.10}
    assert check_net_interchange("n", sums).status == "pass"


def test_net_interchange_flags_imbalance():
    sums = {T0: 33.41, T0 + timedelta(minutes=5): 900.0}
    r = check_net_interchange("n", sums)
    assert r.status == "fail" and "1 interval" in r.detail


def test_exit_code_only_fails_on_fail():
    ok = check_freshness("f", T0, T0)
    warn = check_quarantine_rate("q", 0, 0)
    bad = check_quarantine_rate("q", 1, 1)
    assert exit_code([ok, warn]) == 0
    assert exit_code([ok, warn, bad]) == 1
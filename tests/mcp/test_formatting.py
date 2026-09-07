from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.mcp.formatting import freshness, money, refused


def test_money_renders_a_decimal_as_exact_text() -> None:
    assert money(Decimal("100.25")) == "100.25"


def test_money_passes_a_string_through_untouched() -> None:
    # The gateway already sends money as text; re-parsing risks losing it.
    assert money("1234.5600") == "1234.5600"


def test_money_of_none_is_none() -> None:
    assert money(None) is None


def test_money_never_returns_a_float() -> None:
    assert money(0.1) == "0.1"
    assert isinstance(money(0.1), str)


def test_refused_carries_the_reason_verbatim() -> None:
    result = refused("insufficient cash: order needs 500, portfolio has 100")
    assert result["status"] == "REFUSED"
    assert result["reason"] == "insufficient cash: order needs 500, portfolio has 100"


def test_refused_merges_extra_context() -> None:
    result = refused("market closed", exchange="NSE")
    assert result["exchange"] == "NSE"
    assert result["status"] == "REFUSED"


def test_freshness_of_a_recent_daily_bar_is_not_stale() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(hours=20), "1d", now)
    assert result["stale"] is False
    assert result["warning"] is None
    assert result["age_seconds"] == 72000


def test_freshness_flags_a_daily_bar_older_than_three_days() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(days=17), "1d", now)
    assert result["stale"] is True
    assert "17 days" in str(result["warning"])


def test_freshness_tolerates_a_weekend_on_a_daily_series() -> None:
    # Friday's close read on Monday morning is about 2.7 days old and must
    # not be reported as stale, or every Monday would raise a false alarm.
    now = datetime(2026, 9, 7, 9, 15, tzinfo=UTC)
    result = freshness(now - timedelta(days=2, hours=18), "1d", now)
    assert result["stale"] is False


def test_freshness_flags_a_minute_bar_after_a_few_minutes() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(now - timedelta(minutes=10), "1m", now)
    assert result["stale"] is True


def test_freshness_with_no_bars_is_stale_and_says_so() -> None:
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    result = freshness(None, "1d", now)
    assert result["stale"] is True
    assert result["as_of"] is None
    assert "no bars" in str(result["warning"])

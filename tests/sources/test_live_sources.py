from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.live

RECENT_TRADING_DAY = date(2026, 8, 13)
HISTORICAL_DAY = date(2019, 3, 14)


def test_nse_cm_udiff_url_is_still_valid(tmp_path):
    from trading.sources.http import ArchivingClient
    from trading.sources.nse_udiff import NseUdiffSource

    src = NseUdiffSource("cm", client=ArchivingClient(root=tmp_path))
    payload = src.fetch(RECENT_TRADING_DAY)
    assert payload is not None and len(payload.content) > 10_000


def test_amfi_url_is_still_valid(tmp_path):
    """AmfiNavSource is forward-only, so it must be exercised with today's date."""
    from trading.sources.amfi import AmfiNavSource
    from trading.sources.http import ArchivingClient

    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    payload = AmfiNavSource(client=ArchivingClient(root=tmp_path)).fetch(today)
    assert payload is not None and b"Scheme Code" in payload.content[:200]


def test_amfi_history_url_is_still_valid(tmp_path):
    from trading.sources.amfi_history import AmfiNavHistorySource
    from trading.sources.http import ArchivingClient

    payload = AmfiNavHistorySource(client=ArchivingClient(root=tmp_path)).fetch(HISTORICAL_DAY)
    assert payload is not None and b"14-Mar-2019" in payload.content

from datetime import date

import pytest

pytestmark = pytest.mark.live

RECENT_TRADING_DAY = date(2026, 8, 13)


def test_nse_cm_udiff_url_is_still_valid(tmp_path):
    from trading.sources.http import ArchivingClient
    from trading.sources.nse_udiff import NseUdiffSource

    src = NseUdiffSource("cm", client=ArchivingClient(root=tmp_path))
    payload = src.fetch(RECENT_TRADING_DAY)
    assert payload is not None and len(payload.content) > 10_000


def test_amfi_url_is_still_valid(tmp_path):
    from trading.sources.amfi import AmfiNavSource
    from trading.sources.http import ArchivingClient

    payload = AmfiNavSource(client=ArchivingClient(root=tmp_path)).fetch(RECENT_TRADING_DAY)
    assert payload is not None and b"Scheme Code" in payload.content[:200]

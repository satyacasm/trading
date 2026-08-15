from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from trading.sources.amfi import AmfiNavSource
from trading.sources.amfi_history import AmfiNavHistorySource
from trading.sources.bse_udiff import BseUdiffSource
from trading.sources.http import ArchivingClient
from trading.sources.nse_legacy import NseLegacyCmSource
from trading.sources.nse_udiff import NseUdiffSource

IST = ZoneInfo("Asia/Kolkata")


def _client(tmp_path: Path, handler) -> ArchivingClient:
    return ArchivingClient(root=tmp_path, transport=httpx.MockTransport(handler))


def test_nse_cm_udiff_builds_the_documented_url_and_primes_with_cookie(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"zipbytes")

    src = NseUdiffSource("cm", client=_client(tmp_path, handler))
    payload = src.fetch(date(2026, 8, 13))

    assert src.source_key == "nse_cm_udiff"
    assert payload is not None
    assert payload.source_key == "nse_cm_udiff"
    assert payload.content == b"zipbytes"
    assert [str(r.url) for r in seen] == [
        "https://www.nseindia.com",
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20260813_F_0000.csv.zip",
    ]
    assert seen[1].headers["referer"] == "https://www.nseindia.com/"
    assert payload.archive_path == tmp_path / "nse_cm_udiff" / "2026" / "08" / "2026-08-13.zip"
    assert payload.archive_path.read_bytes() == b"zipbytes"


def test_nse_fo_udiff_uses_the_fo_path_segment(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"foobytes")

    src = NseUdiffSource("fo", client=_client(tmp_path, handler))
    payload = src.fetch(date(2026, 8, 13))

    assert src.source_key == "nse_fo_udiff"
    assert payload is not None and payload.source_key == "nse_fo_udiff"
    assert str(seen[1].url) == (
        "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_20260813_F_0000.csv.zip"
    )


def test_nse_udiff_returns_none_on_holiday_404(tmp_path: Path) -> None:
    src = NseUdiffSource("cm", client=_client(tmp_path, lambda req: httpx.Response(404)))
    assert src.fetch(date(2026, 1, 26)) is None


def test_bse_udiff_builds_url_with_no_priming(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"bsebytes")

    src = BseUdiffSource(client=_client(tmp_path, handler))
    payload = src.fetch(date(2026, 8, 13))

    assert src.source_key == "bse_cm_udiff"
    assert len(seen) == 1  # no priming request
    assert str(seen[0].url) == (
        "https://www.bseindia.com/download/BhavCopy/Equity/"
        "BhavCopy_BSE_CM_0_0_0_20260813_F_0000.CSV"
    )
    assert seen[0].headers["referer"] == "https://www.bseindia.com/"
    assert payload is not None
    assert payload.archive_path == tmp_path / "bse_cm_udiff" / "2026" / "08" / "2026-08-13.csv"


def test_nse_legacy_url_uses_uppercase_three_letter_month(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"legacybytes")

    src = NseLegacyCmSource(client=_client(tmp_path, handler))
    payload = src.fetch(date(2019, 3, 14))

    assert src.source_key == "nse_cm_legacy"
    assert [str(r.url) for r in seen] == [
        "https://www.nseindia.com",
        "https://nsearchives.nseindia.com/content/historical/EQUITIES/"
        "2019/MAR/cm14MAR2019bhav.csv.zip",
    ]
    assert payload is not None
    assert payload.archive_path == tmp_path / "nse_cm_legacy" / "2019" / "03" / "2019-03-14.zip"


def test_amfi_source_hits_the_portal_host_with_no_priming(tmp_path: Path) -> None:
    today = datetime.now(IST).date()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"Scheme Code;...")

    src = AmfiNavSource(client=_client(tmp_path, handler))
    payload = src.fetch(today)

    assert src.source_key == "amfi_nav"
    assert len(seen) == 1
    assert str(seen[0].url) == "https://portal.amfiindia.com/spages/NAVAll.txt"
    assert payload is not None
    assert payload.archive_path == (
        tmp_path / "amfi_nav" / f"{today:%Y}" / f"{today:%m}" / f"{today.isoformat()}.txt"
    )


def test_amfi_source_returns_none_when_empty_404(tmp_path: Path) -> None:
    today = datetime.now(IST).date()
    src = AmfiNavSource(client=_client(tmp_path, lambda req: httpx.Response(404)))
    assert src.fetch(today) is None


def test_amfi_source_refuses_a_past_date_without_any_http_call(tmp_path: Path) -> None:
    """A past date is not "no data" from a server round trip - it must never even ask.

    NAVAll.txt is a latest-snapshot feed: it would happily return 200 with
    today's content for a request tagged with yesterday's date, silently
    corrupting the ingest-job ledger. The guard must short-circuit before any
    HTTP request is made.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"AmfiNavSource must not make an HTTP request: {request.url}")

    yesterday = datetime.now(IST).date() - timedelta(days=1)
    src = AmfiNavSource(client=_client(tmp_path, handler))

    assert src.fetch(yesterday) is None


def test_amfi_source_refuses_a_future_date_without_any_http_call(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"AmfiNavSource must not make an HTTP request: {request.url}")

    tomorrow = datetime.now(IST).date() + timedelta(days=1)
    src = AmfiNavSource(client=_client(tmp_path, handler))

    assert src.fetch(tomorrow) is None


def test_amfi_history_source_builds_the_documented_url_with_ddmonyyyy_dates(
    tmp_path: Path,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"14-Mar-2019 data")

    src = AmfiNavHistorySource(client=_client(tmp_path, handler))
    payload = src.fetch(date(2019, 3, 14))

    assert src.source_key == "amfi_nav_history"
    assert len(seen) == 1  # no priming request
    assert str(seen[0].url) == (
        "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
        "?frmdt=14-Mar-2019&todt=14-Mar-2019"
    )
    assert payload is not None
    assert payload.source_key == "amfi_nav_history"
    assert payload.archive_path == (
        tmp_path / "amfi_nav_history" / "2019" / "03" / "2019-03-14.txt"
    )


def test_amfi_history_source_returns_none_on_404(tmp_path: Path) -> None:
    src = AmfiNavHistorySource(client=_client(tmp_path, lambda req: httpx.Response(404)))
    assert src.fetch(date(2019, 3, 14)) is None

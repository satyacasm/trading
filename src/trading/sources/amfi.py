from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
IST = ZoneInfo("Asia/Kolkata")


class AmfiNavSource:
    """Fetches AMFI's daily *latest-snapshot* NAV file.

    This is the forward-only daily source: the file carries the latest NAV per
    scheme rather than one date's worth of history, so it cannot answer for a
    past business date. Every call hits the same URL, and there is no way to
    tell from the response alone which date it belongs to — so `fetch` refuses
    (returns None) for any business_date that is not "today" in IST, the
    market's calendar day. Without this guard, a backfill run would archive
    today's snapshot under a past date and mark that date's ingest job
    SUCCESS, silently corrupting the job ledger that drives backfill
    resumption. For historical dates, use `AmfiNavHistorySource` instead.
    """

    source_key = "amfi_nav"

    def __init__(self, client: ArchivingClient | None = None):
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        if business_date != datetime.now(IST).date():
            return None
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.txt"
        )
        result = self._client.get(URL, archive_name=name, prime=None)
        if result is None:
            return None
        body, path, digest = result
        return RawPayload(
            source_key=self.source_key,
            business_date=business_date,
            content=body,
            content_hash=digest,
            fetched_at=datetime.now(UTC),
            archive_path=path,
            meta={"url": URL},
        )

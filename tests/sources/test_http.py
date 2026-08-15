import hashlib
from pathlib import Path

import httpx
import pytest

from trading.contracts import FetchError
from trading.sources.http import ArchivingClient


def test_get_writes_the_body_to_the_archive_and_returns_it(tmp_path: Path) -> None:
    body = b"col_a,col_b\n1,2\n"
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=body))
    client = ArchivingClient(root=tmp_path, transport=transport)

    got, path, digest = client.get("https://example.test/f.csv", archive_name="f.csv")

    assert got == body
    assert path.read_bytes() == body
    assert digest == hashlib.sha256(body).hexdigest()


def test_get_retries_then_succeeds(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, content=b"ok")

    client = ArchivingClient(
        root=tmp_path, transport=httpx.MockTransport(handler), backoff_seconds=0.0
    )
    body, _, _ = client.get("https://example.test/f", archive_name="f")

    assert body == b"ok"
    assert calls["n"] == 3


def test_get_raises_fetch_error_after_exhausting_retries(tmp_path: Path) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(503))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    with pytest.raises(FetchError):
        client.get("https://example.test/f", archive_name="f")


def test_404_returns_none_rather_than_raising(tmp_path: Path) -> None:
    """A holiday is ordinary control flow, not an exception (spec 5.4)."""
    transport = httpx.MockTransport(lambda req: httpx.Response(404))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    assert client.get("https://example.test/f", archive_name="f") is None


def test_empty_body_on_200_is_a_fetch_error(tmp_path: Path) -> None:
    """An empty file on a trading day is suspicious, never success."""
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=b""))
    client = ArchivingClient(root=tmp_path, transport=transport, backoff_seconds=0.0)

    with pytest.raises(FetchError):
        client.get("https://example.test/f", archive_name="f")


def test_priming_request_is_made_before_the_real_one(tmp_path: Path) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, content=b"data")

    client = ArchivingClient(root=tmp_path, transport=httpx.MockTransport(handler))
    client.get(
        "https://nsearchives.nseindia.com/x.zip",
        archive_name="x.zip",
        prime="https://www.nseindia.com",
    )

    assert seen == ["https://www.nseindia.com", "https://nsearchives.nseindia.com/x.zip"]


def test_a_transport_level_priming_failure_does_not_abort_the_real_request(
    tmp_path: Path,
) -> None:
    """A DNS blip or connection reset on the prime leg (nseindia.com is flaky) must not
    escape as a bare httpx exception - it should be swallowed and the main request tried
    anyway, per the taxonomy None/FetchError that the backfill's job ledger depends on.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == "https://www.nseindia.com":
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(200, content=b"data")

    client = ArchivingClient(root=tmp_path, transport=httpx.MockTransport(handler))
    body, _, _ = client.get(
        "https://nsearchives.nseindia.com/x.zip",
        archive_name="x.zip",
        prime="https://www.nseindia.com",
    )

    assert body == b"data"


def test_transport_level_failure_on_both_legs_raises_fetch_error_not_a_bare_httpx_error(
    tmp_path: Path,
) -> None:
    """The taxonomy only has None and FetchError; a raw httpx.HTTPError escaping would
    crash a long backfill run instead of recording the job as FAILED and moving on.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection reset", request=request)

    client = ArchivingClient(
        root=tmp_path, transport=httpx.MockTransport(handler), backoff_seconds=0.0
    )

    with pytest.raises(FetchError):
        client.get(
            "https://nsearchives.nseindia.com/x.zip",
            archive_name="x.zip",
            prime="https://www.nseindia.com",
        )

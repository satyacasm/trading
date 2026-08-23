"""Tests for `trading.auth.upstox` (Upstox OAuth token minting).

The network call is driven through an httpx MockTransport rather than a
stubbed function, so the request this code actually builds -- URL, form
encoding, headers -- is what gets asserted.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from trading.auth.upstox import (
    UpstoxAuthError,
    authorize_url,
    exchange_code_for_token,
    write_token,
)


def test_authorize_url_carries_the_client_id_and_redirect():
    url = authorize_url("KEY123", "http://localhost:3000/callback")
    assert url.startswith("https://api.upstox.com/v2/login/authorization/dialog?")
    assert "client_id=KEY123" in url
    assert "response_type=code" in url
    # The redirect must be percent-encoded or Upstox reads it as extra params.
    assert "redirect_uri=http%3A%2F%2Flocalhost%3A3000%2Fcallback" in url


def test_exchange_posts_the_documented_form_and_returns_the_token():
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.content.decode()
        seen["accept"] = request.headers.get("accept")
        return httpx.Response(200, json={"access_token": "tok-abc", "user_id": "U1"})

    token = exchange_code_for_token(
        code="CODE1",
        api_key="KEY123",
        api_secret="SECRET1",
        redirect_uri="http://localhost:3000/callback",
        transport=httpx.MockTransport(handler),
    )

    assert token == "tok-abc"
    assert seen["url"] == "https://api.upstox.com/v2/login/authorization/token"
    assert seen["accept"] == "application/json"
    body = str(seen["body"])
    assert "grant_type=authorization_code" in body
    assert "code=CODE1" in body
    assert "client_id=KEY123" in body
    assert "client_secret=SECRET1" in body


def test_exchange_reports_upstox_error_text_rather_than_a_bare_status():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"errors": [{"message": "Invalid redirect_uri", "errorCode": "UDAPI100068"}]},
        )

    with pytest.raises(UpstoxAuthError, match="Invalid redirect_uri"):
        exchange_code_for_token(
            code="BAD",
            api_key="KEY123",
            api_secret="SECRET1",
            redirect_uri="http://localhost:3000/callback",
            transport=httpx.MockTransport(handler),
        )


def test_exchange_rejects_a_success_response_with_no_token():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"user_id": "U1"})

    with pytest.raises(UpstoxAuthError, match="no access_token"):
        exchange_code_for_token(
            code="C",
            api_key="K",
            api_secret="S",
            redirect_uri="http://localhost:3000/callback",
            transport=httpx.MockTransport(handler),
        )


def test_write_token_creates_the_file_when_absent(tmp_path: Path):
    dest = tmp_path / ".env.local"
    write_token(dest, "tok-1")
    assert dest.read_text() == "UPSTOX_ACCESS_TOKEN=tok-1\n"


def test_write_token_replaces_yesterdays_token_without_duplicating_it(tmp_path: Path):
    dest = tmp_path / ".env.local"
    dest.write_text("UPSTOX_ACCESS_TOKEN=old\nOTHER=keep\n")

    write_token(dest, "tok-2")

    assert dest.read_text() == "UPSTOX_ACCESS_TOKEN=tok-2\nOTHER=keep\n"


def test_write_token_preserves_unrelated_settings(tmp_path: Path):
    dest = tmp_path / ".env.local"
    dest.write_text("UPSTOX_RECORDER_UNIVERSE=NSE_INDEX|Nifty 50\n")

    write_token(dest, "tok-3")

    text = dest.read_text()
    assert "UPSTOX_RECORDER_UNIVERSE=NSE_INDEX|Nifty 50\n" in text
    assert "UPSTOX_ACCESS_TOKEN=tok-3\n" in text


def test_write_token_tolerates_a_file_with_no_trailing_newline(tmp_path: Path):
    dest = tmp_path / ".env.local"
    dest.write_text("OTHER=keep")

    write_token(dest, "tok-4")

    assert dest.read_text() == "OTHER=keep\nUPSTOX_ACCESS_TOKEN=tok-4\n"

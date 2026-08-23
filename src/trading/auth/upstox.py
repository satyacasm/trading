"""Mint an Upstox access token via their OAuth2 authorisation-code flow.

The API key and secret alone cannot open the market feed. Upstox issues a
short-lived bearer token that the recorder reads as `UPSTOX_ACCESS_TOKEN`,
and that token expires daily -- so this is a step before every session, not
a one-time paste, which is exactly why it is a tool rather than a note in a
README.

Run it with:
    uv run python -m trading.auth.upstox

It opens the Upstox login in a browser, catches the redirect on a local
one-shot HTTP server, exchanges the code, and writes the token into
`.env.local` (gitignored) leaving every other setting in place.

The redirect URI must match what is registered in the Upstox developer
console EXACTLY, including scheme, port and path -- a mismatch is the single
most common failure here, and Upstox reports it as UDAPI100068.
"""

from __future__ import annotations

import argparse
import sys
import threading
import webbrowser
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

AUTHORIZE_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"
DEFAULT_REDIRECT_URI = "http://localhost:3000/callback"
TOKEN_ENV_VAR = "UPSTOX_ACCESS_TOKEN"


class UpstoxAuthError(RuntimeError):
    """Upstox refused the exchange, or returned something unusable."""


def authorize_url(api_key: str, redirect_uri: str) -> str:
    """The URL the user logs in at. `urlencode` is doing real work here: an
    unencoded redirect_uri's own query separators would be read as extra
    parameters of this URL."""
    query = urlencode({"client_id": api_key, "redirect_uri": redirect_uri, "response_type": "code"})
    return f"{AUTHORIZE_URL}?{query}"


def _error_text(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or f"HTTP {response.status_code}"
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if isinstance(errors, list) and errors:
        return "; ".join(
            f"{e.get('message', e)}" + (f" ({e['errorCode']})" if e.get("errorCode") else "")
            for e in errors
            if isinstance(e, dict)
        )
    return str(payload)


def exchange_code_for_token(
    *,
    code: str,
    api_key: str,
    api_secret: str,
    redirect_uri: str,
    transport: httpx.BaseTransport | None = None,
) -> str:
    """Trade the one-time authorisation code for a bearer token."""
    with httpx.Client(transport=transport, timeout=30) as client:
        response = client.post(
            TOKEN_URL,
            data={
                "code": code,
                "client_id": api_key,
                "client_secret": api_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"accept": "application/json"},
        )

    if response.status_code >= 400:
        raise UpstoxAuthError(f"Upstox refused the token exchange: {_error_text(response)}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise UpstoxAuthError(f"Upstox returned non-JSON: {response.text[:200]!r}") from exc

    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not token:
        raise UpstoxAuthError(
            f"Upstox accepted the exchange but returned no access_token: {payload}"
        )
    return str(token)


def write_token(env_path: Path, token: str) -> None:
    """Upsert `UPSTOX_ACCESS_TOKEN` into an env file, in place.

    Replaces yesterday's line rather than appending, since this runs daily
    and a file accumulating stale tokens would leave which one wins up to
    the parser.
    """
    line = f"{TOKEN_ENV_VAR}={token}"
    existing = env_path.read_text().splitlines() if env_path.exists() else []
    replaced = False
    out = []
    for entry in existing:
        if entry.startswith(f"{TOKEN_ENV_VAR}="):
            out.append(line)
            replaced = True
        else:
            out.append(entry)
    if not replaced:
        out.append(line)
    env_path.write_text("\n".join(out) + "\n")


# --------------------------------------------------------------------------
# One-shot local server to catch the redirect
# --------------------------------------------------------------------------


class _CodeCatcher(BaseHTTPRequestHandler):
    code: str | None = None
    error: str | None = None

    def do_GET(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        params = parse_qs(urlparse(self.path).query)

        def first(*names: str) -> str | None:
            for n in names:
                values = params.get(n)
                if values:
                    return values[0]
            return None

        _CodeCatcher.code = first("code")
        _CodeCatcher.error = first("error_description", "error")
        body = (
            b"<h2>Upstox connected.</h2><p>You can close this tab and return to the terminal.</p>"
            if _CodeCatcher.code
            else b"<h2>No authorisation code received.</h2><p>Check the terminal.</p>"
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        """Silence the default stderr access log; it only adds noise here."""


def _await_code(redirect_uri: str, timeout: float) -> str:
    parsed = urlparse(redirect_uri)
    server = HTTPServer((parsed.hostname or "localhost", parsed.port or 80), _CodeCatcher)
    server.timeout = timeout
    _CodeCatcher.code = _CodeCatcher.error = None

    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    thread.join(timeout)
    server.server_close()

    if _CodeCatcher.error:
        raise UpstoxAuthError(f"Upstox returned an error at the redirect: {_CodeCatcher.error}")
    if not _CodeCatcher.code:
        raise UpstoxAuthError(
            f"No authorisation code arrived at {redirect_uri} within {timeout:.0f}s. "
            "The usual cause is a redirect URI that does not match the Upstox "
            "developer console exactly (scheme, port and path all count)."
        )
    return _CodeCatcher.code


def main(argv: Sequence[str] | None = None) -> int:
    from trading.config import get_settings

    parser = argparse.ArgumentParser(description="Mint an Upstox access token.")
    parser.add_argument(
        "--redirect-uri",
        default=DEFAULT_REDIRECT_URI,
        help="Must match the Upstox developer console exactly (default: %(default)s).",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args(argv)

    settings = get_settings()
    if not settings.upstox_api_key or not settings.upstox_api_secret:
        print(
            "UPSTOX_API_KEY / UPSTOX_API_SECRET are not set. Add them to .env "
            "(they come from the Upstox developer console, not your login).",
            file=sys.stderr,
        )
        return 2

    url = authorize_url(settings.upstox_api_key, args.redirect_uri)
    print("Opening the Upstox login in your browser.")
    print(f"If it does not open, paste this in yourself:\n\n  {url}\n")
    webbrowser.open(url)

    try:
        code = _await_code(args.redirect_uri, args.timeout)
        token = exchange_code_for_token(
            code=code,
            api_key=settings.upstox_api_key,
            api_secret=settings.upstox_api_secret,
            redirect_uri=args.redirect_uri,
        )
    except UpstoxAuthError as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 1

    write_token(args.env_file, token)
    print(f"Token written to {args.env_file} as {TOKEN_ENV_VAR}.")
    print("It expires daily -- re-run this before each session.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

from decimal import Decimal
from pathlib import Path

from trading.config import Settings

# NOTE: every Settings() in this file passes _env_file=None.
# Settings normally reads .env and .env.local. Without this, the moment real
# broker credentials are added to .env.local, `assert s.upstox_api_key is None`
# starts failing — a confusing breakage at exactly the wrong time. Tests must
# read the environment they set, and nothing else.


def test_settings_read_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.database_url == "postgresql://u:p@localhost:5432/db"
    assert s.data_root == Path(tmp_path)
    assert s.upstox_api_key is None  # optional until credentials arrive


def test_data_subdirectories_are_derived(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.raw_archive_root == tmp_path / "raw"
    assert s.recordings_root == tmp_path / "recordings"


def test_real_credentials_in_env_local_do_not_break_the_suite(monkeypatch, tmp_path):
    """Regression guard: adding a real broker key must not fail unrelated tests."""
    env_local = tmp_path / ".env.local"
    env_local.write_text("UPSTOX_API_KEY=a-real-looking-key\n")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.upstox_api_key is None


def test_paper_slippage_bps_defaults_to_five(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.paper_slippage_bps == Decimal("5")


def test_paper_slippage_bps_is_tunable_via_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("PAPER_SLIPPAGE_BPS", "12.5")

    s = Settings(_env_file=None)

    assert s.paper_slippage_bps == Decimal("12.5")


def test_telegram_settings_default_to_none(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.telegram_bot_token is None  # unconfigured bot is a valid state, not an error
    assert s.telegram_chat_id is None


def test_telegram_settings_are_tunable_via_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "987654")

    s = Settings(_env_file=None)

    assert s.telegram_bot_token == "123456:test-token"
    assert s.telegram_chat_id == "987654"


def test_cors_origins_default_to_the_web_apps_port(monkeypatch) -> None:  # noqa: ANN001
    """Unset, the gateway answers the web app's own origin -- port 3010
    (3000 is taken by another project on this machine), under both
    spellings a browser may use for it."""
    from trading.config import Settings

    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("REDIS_URL", "redis://x")
    assert Settings().cors_allow_origins.split(",") == [
        "http://localhost:3010",
        "http://127.0.0.1:3010",
    ]


def test_cors_origins_accept_a_comma_separated_list(monkeypatch) -> None:  # noqa: ANN001
    """So a second dev server -- a worktree verifying its own branch -- can be
    allowed without editing code."""
    from trading.config import Settings

    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("REDIS_URL", "redis://x")
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "http://localhost:3000,http://localhost:3001")
    assert Settings().cors_allow_origins.split(",") == [
        "http://localhost:3000",
        "http://localhost:3001",
    ]


def test_live_resilience_thresholds_have_spec_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings(_env_file=None)

    assert s.backfill_silence_seconds == 90
    assert s.backfill_sweep_seconds == 300
    assert s.backfill_sweep_window_minutes == 30
    assert s.live_delivery_timer_seconds == 30
    assert s.live_catchup_after_seconds == 120
    assert s.live_replay_cap_hours == 24
    assert s.live_state_max_bytes == 65536
    assert s.live_reply_timeout_seconds == 30
    assert s.stale_price_seconds == 180
    assert s.heartbeat_ttl_seconds == 30
    assert s.heartbeat_refresh_seconds == 10


def test_gateway_url_defaults_to_loopback_8010_and_mcp_follows_it(monkeypatch, tmp_path):
    """8000 collides with another project's container on this machine, and
    `localhost` can resolve to ::1 where that container listens -- so the
    gateway lives on 127.0.0.1:8010, and the MCP server finds it there."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("GATEWAY_URL", raising=False)
    monkeypatch.delenv("MCP_GATEWAY_URL", raising=False)

    s = Settings(_env_file=None)

    assert s.gateway_url == "http://127.0.0.1:8010"
    assert s.mcp_gateway_url == s.gateway_url

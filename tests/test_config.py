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

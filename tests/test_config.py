from pathlib import Path

from trading.config import Settings


def test_settings_read_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings()

    assert s.database_url == "postgresql://u:p@localhost:5432/db"
    assert s.data_root == Path(tmp_path)
    assert s.upstox_api_key is None  # optional until credentials arrive


def test_data_subdirectories_are_derived(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))

    s = Settings()

    assert s.raw_archive_root == tmp_path / "raw"
    assert s.recordings_root == tmp_path / "recordings"

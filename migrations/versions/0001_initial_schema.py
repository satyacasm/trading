"""Initial schema: instrument master, TimescaleDB hypertables, operational tables.

Transcribed from docs/superpowers/specs/2026-08-14-phase-0-data-foundations-design.md
sections 4.1-4.6.

Revision ID: 0001
Revises:
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from trading.contracts import DataSource

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    # --- §4.6 users (created first: no dependencies, empty in Phase 0, D3) ---
    op.execute(
        """
        CREATE TABLE users (            -- empty in Phase 0; D3
            user_id    BIGSERIAL PRIMARY KEY,
            email      TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    # --- §4.5 data_sources lookup table (source of `bars_daily.source`) ---
    op.execute(
        """
        CREATE TABLE data_sources (
            source_id   SMALLINT PRIMARY KEY,
            source_key  TEXT NOT NULL UNIQUE,
            description TEXT
        )
        """
    )
    # Seeded by iterating the Python enum so the two can never drift.
    for source in DataSource:
        op.execute(
            f"INSERT INTO data_sources (source_id, source_key) "
            f"VALUES ({source.value}, '{source.name}')"
        )

    # --- §4.1 Instrument master ---
    op.execute(
        """
        CREATE TABLE instruments (
            instrument_id   BIGSERIAL PRIMARY KEY,
            asset_class     TEXT        NOT NULL,   -- EQUITY|INDEX|FUTURE|OPTION|MF|CRYPTO|COMM
            exchange        TEXT        NOT NULL,   -- NSE|BSE|MCX|BINANCE|NASDAQ
            segment         TEXT        NOT NULL,   -- CM|FO|MF|CD|COM
            symbol          TEXT        NOT NULL,
            underlying_id   BIGINT      REFERENCES instruments(instrument_id),
            expiry          DATE,
            strike          NUMERIC(18,4),
            option_type     TEXT,                   -- CE|PE|NULL
            tick_size       NUMERIC(12,6),
            currency        TEXT        NOT NULL DEFAULT 'INR',
            isin            TEXT,
            name            TEXT,
            listed_on       DATE,                   -- survivorship-bias defence
            delisted_on     DATE,                   -- delisted rows are never deleted
            status          TEXT        NOT NULL,   -- ACTIVE|EXPIRED|DELISTED|SUSPENDED
            source_bindings JSONB       NOT NULL DEFAULT '{}'::jsonb,
            canonical_key   TEXT        NOT NULL,   -- D6: app-maintained
            first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

            CONSTRAINT uq_instrument_natural
                UNIQUE NULLS NOT DISTINCT (exchange, segment, symbol, expiry, strike, option_type),
            CONSTRAINT uq_instrument_canonical UNIQUE (canonical_key),
            CONSTRAINT ck_option_fields CHECK (
                (asset_class = 'OPTION') = (option_type IS NOT NULL AND strike IS NOT NULL)
            ),
            CONSTRAINT ck_derivative_expiry CHECK (
                (asset_class IN ('OPTION','FUTURE')) = (expiry IS NOT NULL)
            )
        )
        """
    )
    op.execute("CREATE INDEX ix_instruments_symbol   ON instruments (exchange, segment, symbol)")
    op.execute(
        "CREATE INDEX ix_instruments_underlying ON instruments (underlying_id) "
        "WHERE underlying_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX ix_instruments_expiry   ON instruments (expiry) WHERE expiry IS NOT NULL"
    )
    op.execute("CREATE INDEX ix_instruments_isin     ON instruments (isin) WHERE isin IS NOT NULL")

    # --- §4.2 Temporal attributes ---
    op.execute(
        """
        CREATE TABLE instrument_lot_history (
            instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id),
            effective_from DATE   NOT NULL,
            effective_to   DATE,               -- NULL = current
            lot_size       INTEGER NOT NULL CHECK (lot_size > 0),
            source         TEXT   NOT NULL,
            PRIMARY KEY (instrument_id, effective_from),
            CONSTRAINT ck_lot_range CHECK (effective_to IS NULL OR effective_to > effective_from)
        )
        """
    )

    # --- §4.3 Corporate actions ---
    op.execute(
        """
        CREATE TABLE corporate_actions (
            action_id     BIGSERIAL PRIMARY KEY,
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
            action_type   TEXT   NOT NULL,   -- SPLIT|BONUS|DIVIDEND|RIGHTS|MERGER|DEMERGER|
                                              -- SYMBOL_CHANGE|FACE_VALUE_CHANGE
            ex_date       DATE   NOT NULL,
            record_date   DATE,
            ratio_from    NUMERIC(18,6),     -- SPLIT 1:5 => from=1, to=5
            ratio_to      NUMERIC(18,6),
            amount        NUMERIC(18,4),     -- DIVIDEND per share
            new_symbol    TEXT,              -- SYMBOL_CHANGE
            announced_at  TIMESTAMPTZ,       -- point-in-time: when WE learned it
            source        TEXT   NOT NULL,
            raw           JSONB,
            ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # NOTE: this must be a unique INDEX, not a table-level UNIQUE constraint.
    # Postgres does not permit expressions in UNIQUE constraints, only in indexes.
    op.execute(
        """
        CREATE UNIQUE INDEX uq_corp_action ON corporate_actions
            (instrument_id, action_type, ex_date, (COALESCE(ratio_to, amount, 0)))
        """
    )
    op.execute("CREATE INDEX ix_corp_actions_lookup ON corporate_actions (instrument_id, ex_date)")

    # --- §4.4 Trading calendar ---
    op.execute(
        """
        CREATE TABLE trading_calendar (
            exchange       TEXT NOT NULL,
            segment        TEXT NOT NULL,
            session_date   DATE NOT NULL,
            is_trading_day BOOLEAN NOT NULL,
            session_open   TIME,
            session_close  TIME,
            note           TEXT,             -- 'Diwali Muhurat', 'Unscheduled closure'
            PRIMARY KEY (exchange, segment, session_date)
        )
        """
    )

    # --- §4.5 Price hypertables ---
    op.execute(
        """
        CREATE TABLE bars_daily (
            instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
            ts            TIMESTAMPTZ NOT NULL,   -- bar CLOSE time (parent §6)
            open          NUMERIC(18,4) NOT NULL,
            high          NUMERIC(18,4) NOT NULL,
            low           NUMERIC(18,4) NOT NULL,
            close         NUMERIC(18,4) NOT NULL,
            prev_close    NUMERIC(18,4),
            volume        BIGINT,
            turnover      NUMERIC(22,4),
            trades        INTEGER,
            -- F&O only
            settle_price  NUMERIC(18,4),
            open_interest BIGINT,
            oi_change     BIGINT,
            -- equity only (arrives from a separate NSE file, lands via upsert)
            delivery_qty  BIGINT,
            delivery_pct  NUMERIC(7,4),
            extra         JSONB,
            source        SMALLINT    NOT NULL,
            ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (instrument_id, ts),
            -- Conditioned on positive volume: untraded F&O contracts publish
            -- OHLC = 0 with a theoretically-derived non-zero close. See finding F2 —
            -- an unconditional constraint rejects ~60% of F&O rows.
            CONSTRAINT ck_ohlc_order CHECK (
                volume IS NULL OR volume = 0 OR (
                    high >= low AND high >= open AND high >= close
                    AND low <= open AND low <= close
                )
            ),
            CONSTRAINT ck_close_positive CHECK (close > 0)
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('bars_daily', 'ts', chunk_time_interval => INTERVAL '1 month')"
    )
    op.execute(
        """
        ALTER TABLE bars_daily SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'instrument_id',
            timescaledb.compress_orderby   = 'ts DESC'
        )
        """
    )
    op.execute("SELECT add_compression_policy('bars_daily', INTERVAL '3 months')")

    # bars_intraday: same shape as bars_daily, chunked daily, with interval_sec added
    # to the primary key. Created empty in Phase 0 so Phase 1 requires no migration.
    op.execute(
        """
        CREATE TABLE bars_intraday (
            instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
            ts            TIMESTAMPTZ NOT NULL,
            interval_sec  SMALLINT    NOT NULL,
            open          NUMERIC(18,4) NOT NULL,
            high          NUMERIC(18,4) NOT NULL,
            low           NUMERIC(18,4) NOT NULL,
            close         NUMERIC(18,4) NOT NULL,
            prev_close    NUMERIC(18,4),
            volume        BIGINT,
            turnover      NUMERIC(22,4),
            trades        INTEGER,
            settle_price  NUMERIC(18,4),
            open_interest BIGINT,
            oi_change     BIGINT,
            delivery_qty  BIGINT,
            delivery_pct  NUMERIC(7,4),
            extra         JSONB,
            source        SMALLINT    NOT NULL,
            ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (instrument_id, ts, interval_sec),
            CONSTRAINT ck_ohlc_order_intraday CHECK (
                volume IS NULL OR volume = 0 OR (
                    high >= low AND high >= open AND high >= close
                    AND low <= open AND low <= close
                )
            ),
            CONSTRAINT ck_close_positive_intraday CHECK (close > 0)
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('bars_intraday', 'ts', chunk_time_interval => INTERVAL '1 day')"
    )

    # --- §4.6 Operational tables ---
    op.execute(
        """
        CREATE TABLE ingest_jobs (
            job_id          BIGSERIAL PRIMARY KEY,
            source_key      TEXT NOT NULL,      -- 'nse_eq_bhavcopy'
            business_date   DATE NOT NULL,
            status          TEXT NOT NULL,      -- PENDING|RUNNING|SUCCESS|FAILED|
                                                 -- SKIPPED_HOLIDAY|SKIPPED_NO_DATA
            attempt         INTEGER NOT NULL DEFAULT 0,
            rows_written    BIGINT,
            quarantine_count INTEGER NOT NULL DEFAULT 0,
            content_hash    TEXT,               -- detects source restatements
            archive_path    TEXT,
            started_at      TIMESTAMPTZ,
            finished_at     TIMESTAMPTZ,
            error           TEXT,
            CONSTRAINT uq_ingest_job UNIQUE (source_key, business_date)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE quarantine (
            quarantine_id BIGSERIAL PRIMARY KEY,
            job_id        BIGINT NOT NULL REFERENCES ingest_jobs(job_id),
            reason        TEXT   NOT NULL,
            row_payload   JSONB  NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS quarantine")
    op.execute("DROP TABLE IF EXISTS ingest_jobs")
    op.execute("DROP TABLE IF EXISTS bars_intraday")
    op.execute("DROP TABLE IF EXISTS bars_daily")
    op.execute("DROP TABLE IF EXISTS trading_calendar")
    op.execute("DROP TABLE IF EXISTS corporate_actions")
    op.execute("DROP TABLE IF EXISTS instrument_lot_history")
    op.execute("DROP TABLE IF EXISTS instruments")
    op.execute("DROP TABLE IF EXISTS data_sources")
    op.execute("DROP TABLE IF EXISTS users")
    # The timescaledb extension itself is left installed: it is a shared,
    # database-wide resource created with IF NOT EXISTS, and dropping/recreating
    # it on every downgrade/upgrade cycle is unnecessary and riskier than leaving
    # it in place.

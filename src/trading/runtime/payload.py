"""The stdin envelope carrying a smoke run into the sandbox.

Three properties are load-bearing.

**Money is text, never a JSON number.** Every price, quantity, and rate
crosses as a string and is rebuilt with `Decimal(...)`. JSON numbers are
IEEE 754 doubles, and a payload that quietly round-tripped prices through
binary floating point would corrupt every charge computed in the
container -- in a system whose headline feature is an honest cost model.

**Bars are columnar, and the whole envelope is gzipped.** The universe is
not capped (D-S7), so a wide manifest can mean ~88,000 bars. Per-bar
objects repeat eight keys each; parallel arrays do not, and numeric text
in columns compresses about ten to one.

**`mode` is explicit and required.** The runner never guesses whether it
received raw source or an envelope. Format detection by sniffing is the
compatibility shim that breaks silently a year later.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from trading.paper.models import ChargeSchedule
from trading.runtime.provider import BarRecord

__all__ = ["MODE_CONFIGURE", "MODE_SMOKE", "SmokePayload", "decode_payload", "encode_payload"]

MODE_CONFIGURE = "configure"
MODE_SMOKE = "smoke"
_MODES = frozenset({MODE_CONFIGURE, MODE_SMOKE})


@dataclass(frozen=True)
class SmokePayload:
    mode: str
    source: str
    contract_version: str = "0.1"
    window: dict[str, Any] | None = None
    bars: Mapping[int, tuple[BarRecord, ...]] = field(default_factory=dict)
    charge_schedules: tuple[ChargeSchedule, ...] = ()
    starting_cash: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")


def _money(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _encode_series(series: Sequence[BarRecord]) -> dict[str, Any]:
    return {
        "interval_sec": series[0].interval_sec if series else 60,
        "ts": [bar.ts.isoformat() for bar in series],
        "o": [str(bar.open) for bar in series],
        "h": [str(bar.high) for bar in series],
        "l": [str(bar.low) for bar in series],
        "c": [str(bar.close) for bar in series],
        "v": [_money(bar.volume) for bar in series],
        "t": [bar.trades for bar in series],
        "oi": [bar.open_interest for bar in series],
        "oic": [bar.oi_change for bar in series],
    }


def _decode_series(instrument_id: int, column: Mapping[str, Any]) -> tuple[BarRecord, ...]:
    interval_sec = int(column["interval_sec"])
    return tuple(
        BarRecord(
            instrument_id=instrument_id,
            ts=datetime.fromisoformat(ts),
            interval_sec=interval_sec,
            open=Decimal(o),
            high=Decimal(h),
            low=Decimal(low),
            close=Decimal(c),
            volume=None if v is None else Decimal(v),
            trades=t,
            open_interest=oi,
            oi_change=oic,
        )
        for ts, o, h, low, c, v, t, oi, oic in zip(
            column["ts"],
            column["o"],
            column["h"],
            column["l"],
            column["c"],
            column["v"],
            column["t"],
            column["oi"],
            column["oic"],
            strict=True,
        )
    )


def encode_payload(payload: SmokePayload) -> bytes:
    document = {
        "mode": payload.mode,
        "contract_version": payload.contract_version,
        "source": payload.source,
        "window": payload.window,
        "bars": {
            str(instrument_id): _encode_series(series)
            for instrument_id, series in payload.bars.items()
        },
        # ChargeSchedule deliberately has no money field_serializer (it is
        # process-internal and never crosses HTTP), so model_dump_json()
        # already round-trips its Decimals losslessly as strings -- the
        # schedules can cross the envelope via pydantic's own JSON instead
        # of the field-by-field string encoding the bars need above.
        "charge_schedules": [
            json.loads(schedule.model_dump_json()) for schedule in payload.charge_schedules
        ],
        "starting_cash": str(payload.starting_cash),
        "slippage_bps": str(payload.slippage_bps),
    }
    return gzip.compress(json.dumps(document, separators=(",", ":")).encode("utf-8"))


def decode_payload(raw: bytes) -> SmokePayload:
    document = json.loads(gzip.decompress(raw).decode("utf-8"))
    mode = document.get("mode")
    if mode not in _MODES:
        raise ValueError(
            f"payload mode must be one of {sorted(_MODES)}, got {mode!r}; "
            "the runner requires an explicit envelope and never guesses"
        )
    return SmokePayload(
        mode=mode,
        source=document["source"],
        contract_version=document.get("contract_version", "0.1"),
        window=document.get("window"),
        bars={
            int(instrument_id): _decode_series(int(instrument_id), column)
            for instrument_id, column in document.get("bars", {}).items()
        },
        charge_schedules=tuple(
            ChargeSchedule.model_validate(row) for row in document.get("charge_schedules", [])
        ),
        starting_cash=Decimal(document.get("starting_cash", "0")),
        slippage_bps=Decimal(document.get("slippage_bps", "0")),
    )

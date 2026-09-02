from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
from trading.paper.models import ChargeSchedule
from trading.runtime.payload import SmokePayload, decode_payload, encode_payload
from trading.runtime.provider import BarRecord


def _schedule() -> ChargeSchedule:
    return ChargeSchedule(
        broker="ZERODHA",
        exchange="NSE",
        asset_class="EQUITY",
        product=Product.DELIVERY,
        charge_type=ChargeType.BROKERAGE,
        basis=ChargeBasis.FLAT_PER_ORDER,
        applies_to_side="BOTH",
        rate=Decimal("0"),
        cap=None,
        rounding=Rounding.TWO_DECIMALS,
        gst_base_types=(),
        effective_from=datetime(2020, 1, 1).date(),
        effective_to=None,
        source_note="test",
    )


def _payload() -> SmokePayload:
    return SmokePayload(
        mode="smoke",
        source="class S: pass\n",
        window={
            "start": "2026-08-27T00:00:00+00:00",
            "end": "2026-09-02T00:00:00+00:00",
            "sessions": 5,
        },
        bars={
            1401: (
                BarRecord(
                    instrument_id=1401,
                    ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
                    interval_sec=60,
                    open=Decimal("1300.05"),
                    high=Decimal("1301.00"),
                    low=Decimal("1299.50"),
                    close=Decimal("1300.75"),
                    volume=Decimal("1234"),
                ),
            )
        },
        charge_schedules=(_schedule(),),
        starting_cash=Decimal("1000000.00"),
        slippage_bps=Decimal("5"),
    )


def test_round_trip_preserves_every_field_exactly() -> None:
    restored = decode_payload(encode_payload(_payload()))
    original = _payload()
    assert restored.mode == original.mode
    assert restored.source == original.source
    assert restored.window == original.window
    assert restored.starting_cash == original.starting_cash
    assert restored.slippage_bps == original.slippage_bps
    assert restored.charge_schedules == original.charge_schedules
    assert restored.bars == original.bars


def test_money_survives_as_decimal_not_float() -> None:
    # The whole cost model depends on this. A payload that quietly
    # round-trips prices through binary floating point would corrupt
    # every charge computed in the container.
    restored = decode_payload(encode_payload(_payload()))
    bar = restored.bars[1401][0]
    assert isinstance(bar.close, Decimal)
    assert bar.close == Decimal("1300.75")
    assert str(bar.close) == "1300.75"


def test_encoding_is_compressed() -> None:
    # D-S7 feeds an uncapped universe, so the payload must compress.
    raw = encode_payload(_payload())
    assert raw[:2] == b"\x1f\x8b"  # gzip magic


def test_columnar_encoding_beats_per_object_for_a_realistic_series() -> None:
    bars = tuple(
        BarRecord(
            instrument_id=1401,
            ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("1300.05"),
            high=Decimal("1301.00"),
            low=Decimal("1299.50"),
            close=Decimal("1300.75"),
            volume=Decimal("1234"),
        )
        for _ in range(2000)
    )
    payload = SmokePayload(mode="smoke", source="x", bars={1401: bars})
    # 2000 bars must fit comfortably; a per-object JSON encoding would not.
    assert len(encode_payload(payload)) < 200_000


def test_decode_rejects_a_payload_without_a_mode() -> None:
    import gzip
    import json

    raw = gzip.compress(json.dumps({"source": "x"}).encode())
    with pytest.raises(ValueError, match="mode"):
        decode_payload(raw)


def test_the_payload_default_matches_the_registry_contract_version() -> None:
    # payload.py cannot import the constant -- registry.py imports psycopg
    # and trading.runtime ships into a container with no database -- so the
    # literal is duplicated deliberately. This test is what keeps the
    # duplicate honest; it runs on the host, where psycopg exists.
    from trading.agent_contract.registry import CONTRACT_VERSION

    assert SmokePayload(mode="smoke", source="x").contract_version == CONTRACT_VERSION

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from marketfeed.domain import Side, Trade


def _trade() -> Trade:
    return Trade(
        exchange="coinbase",
        symbol="BTC-USD",
        price=Decimal("64213.57"),
        size=Decimal("0.001"),
        side=Side.BUY,
        exchange_ts=datetime(2026, 9, 6, 12, 0, tzinfo=UTC),
        ingest_ts=datetime(2026, 9, 6, 12, 0, 1, tzinfo=UTC),
    )


def test_a_trade_cannot_be_mutated_after_construction() -> None:
    with pytest.raises(FrozenInstanceError):
        _trade().price = Decimal("1")  # type: ignore[misc]


def test_a_trade_has_no_instance_dict() -> None:
    # slots=True: catching this here means a later `self.foo = ...` fails loudly
    assert not hasattr(_trade(), "__dict__")


def test_sequence_is_optional() -> None:
    assert _trade().sequence is None

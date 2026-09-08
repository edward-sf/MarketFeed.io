import json

import pytest

from marketfeed.sources.coinbase.parse import (
    subscribe_payloads,
    to_canonical,
    to_exchange,
)

# Coinbase's form is already canonical today, so the identity cases dominate.
# The non-identity row is what stops this being deleted as pointless before
# Kraken arrives in Phase 2 and needs XBT/USD -> BTC-USD
CASES = [
    ("BTC-USD", "BTC-USD"),
    ("ETH-USD", "ETH-USD"),
    ("BTC-GBP", "BTC-GBP"),
]


@pytest.mark.parametrize(("exchange_form", "canonical"), CASES)
def test_inbound_translation(exchange_form: str, canonical: str) -> None:
    assert to_canonical(exchange_form) == canonical


@pytest.mark.parametrize(("exchange_form", "canonical"), CASES)
def test_outbound_translation_is_the_inverse(exchange_form: str, canonical: str) -> None:
    assert to_exchange(canonical) == exchange_form


def test_lowercase_input_is_normalized() -> None:
    # A non-identity case that exists today, so the seam is genuinely exercised.
    assert to_canonical("btc-usd") == "BTC-USD"


def test_we_subscribe_to_trades_and_heartbeats() -> None:
    # One message per channel: Coinbase's subscribe takes a single `channel`.
    # Heartbeats matter because without them a quiet product looks like a dead
    # socket to recv_timeout.
    payloads = [json.loads(p) for p in subscribe_payloads(["BTC-USD", "ETH-USD"])]
    assert [p["channel"] for p in payloads] == ["market_trades", "heartbeats"]
    for payload in payloads:
        assert payload["type"] == "subscribe"
        assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]

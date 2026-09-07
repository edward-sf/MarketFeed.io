import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from marketfeed.domain import Side, Trade
from marketfeed.errors import MalformedMessageError
from marketfeed.sources.coinbase.messages import TradeBatch
from marketfeed.sources.coinbase.parse import parse_message

INGEST = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
FIXTURE = Path(__file__).parents[2] / "fixtures" / "coinbase" / "trades.jsonl"


def frame(**overrides: object) -> str:
    trade = {
        "trade_id": "1",
        "product_id": "BTC-USD",
        "price": "64213.57",
        "size": "0.001",
        "side": "BUY",
        "time": "2026-09-06T11:59:59.396251Z"
    } | overrides
    return json.dumps(
        {
            "channel": "market_trades",
            "sequence_num": 7,
            "events": [{"type": "update", "trades": [trade]}]
        }
    )


def parse_one(**overrides: object) -> Trade:
    msg = parse_message(frame(**overrides), ingest_ts=INGEST)
    assert isinstance(msg, TradeBatch)
    assert len(msg.trades) == 1
    return msg.trades[0]


def test_a_trade_frame_yields_a_normalized_trade() -> None:
    trade = parse_one()
    assert trade.exchange == "coinbase"
    assert trade.symbol == "BTC-USD"
    assert trade.size == Decimal("0.001")
    assert trade.sequence == 7
    assert trade.ingest_ts == INGEST


def test_price_keeps_precision_that_float_would_destroy() -> None:
    trade = parse_one(price="64213.570000000000001")
    assert trade.price == Decimal("64213.570000000000001")
    assert trade.price != Decimal(float("64213.570000000000001"))


def test_exchange_timestamp_is_timezone_aware_utc() -> None:
    trade = parse_one()
    assert trade.exchange_ts.tzinfo is not None
    assert trade.exchange_ts.utcoffset() == UTC.utcoffset(None)
    assert trade.exchange_ts.year == 2026


def test_one_frame_carrying_several_trades_yields_all_of_them() -> None:
    payload = json.loads(frame())
    payload["events"][0]["trades"] *= 3
    msg = parse_message(json.dumps(payload), ingest_ts=INGEST)
    assert isinstance(msg, TradeBatch)
    assert len(msg.trades) == 3


def test_a_batch_is_reordered_oldest_first() -> None:
    # Coinbase sends trades newest-first within a frame. Left as-is, a conflator
    # keyed on "latest price" would record the OLDEST trade in each frame as
    # current. The parser reverses so downstream sees chronological order.
    payload = json.loads(frame())
    base = payload["events"][0]["trades"][0]
    payload["events"][0]["trades"] = [
        {**base, "time": "2026-09-06T12:00:03Z", "price": "3.00"},
        {**base, "time": "2026-09-06T12:00:02Z", "price": "2.00"},
        {**base, "time": "2026-09-06T12:00:01Z", "price": "1.00"},
    ]
    msg = parse_message(json.dumps(payload), ingest_ts=INGEST)
    assert isinstance(msg, TradeBatch)
    assert [str(t.price) for t in msg.trades] == ["1.00", "2.00", "3.00"]


@pytest.mark.parametrize(
    ("reported", "expected"),
    [("BUY", Side.SELL), ("SELL", Side.BUY)]
)
def test_side_is_normalized_to_the_aggressor(reported: str, expected: Side) -> None:
    assert parse_one(side=reported).side == expected


def test_an_unrecognized_side_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse_one(side="MAYBE")


def test_a_missing_price_is_malformed() -> None:
    payload = json.loads(frame())
    del payload["events"][0]["trades"][0]["price"]
    with pytest.raises(MalformedMessageError):
        parse_message(json.dumps(payload), ingest_ts=INGEST)


def test_a_non_numeric_price_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse_one(price="not-a-number")


def test_a_naive_timestamp_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse_one(time="2026-09-06T11:59:59")


def test_real_captured_frames_all_parse() -> None:
    for line in FIXTURE.read_text().splitlines():
        parse_message(line, ingest_ts=INGEST)

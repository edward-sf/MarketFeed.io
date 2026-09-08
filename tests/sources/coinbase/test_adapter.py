import json
from contextlib import aclosing

import pytest
from websockets.exceptions import ConnectionClosedOK

from marketfeed.domain import Side, Trade
from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase.adapter import CoinbaseAdapter
from tests.fake_exchange import Action, Close, FakeExchange, Send, load_fixtures


def trade_frame(price: str = "64213.57", side: str = "BUY") -> str:
    return json.dumps(
        {
            "channel": "market_trades",
            "sequence_num": 7,
            "events": [
                {
                    "type": "update",
                    "trades": [
                        {
                            "trade_id": "1",
                            "product_id": "BTC-USD",
                            "price": price,
                            "size": "0.001",
                            "side": side,
                            "time": "2026-09-06T11:59:59.396Z"
                        }
                    ],
                }
            ],
        }
    )


def adapter_for(fake: FakeExchange, *, recv_timeout: float = 5.0) -> CoinbaseAdapter:
    return CoinbaseAdapter(
        ["BTC-USD"],
        uri=fake.uri,
        recv_timeout=recv_timeout,
        failures=FailureRateWindow(window_seconds=60.0, ratio=0.5, min_samples=10),
    )


async def drain_into(adapter: CoinbaseAdapter, sink: list[Trade]) -> None:
    """Consume the whole stream into `sink`.

    It takes the list rather than returning one on purpose: every test here ends
    with the stream raising, so a helper that returned its collection would
    never reach the return statement.
    """
    async with aclosing(adapter.stream()) as trades:
        async for trade in trades:
            sink.append(trade)


@pytest.mark.asyncio
async def test_the_adapter_subscribes_to_the_requested_symbols() -> None:
    async with FakeExchange(Close()) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), [])

    sent = [json.loads(frame) for frame in fake.received]
    assert sent == [
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "market_trades"},
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "heartbeats"},
    ]


@pytest.mark.asyncio
async def test_the_adapter_yields_normalized_trades_in_order() -> None:
    seen: list[Trade] = []
    script: list[Action] = [
        Send(trade_frame(price="1.00")),
        Send(trade_frame(price="2.00", side="SELL")),
        Close(),
    ]
    async with FakeExchange(*script) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), seen)

    assert [str(t.price) for t in seen] == ["1.00", "2.00"]
    # Inverted: the frames report Coinbase's maker side BUY then SELL.
    assert [t.side for t in seen] == [Side.SELL, Side.BUY]
    assert all(t.exchange == "coinbase" and t.symbol == "BTC-USD" for t in seen)
    assert all(t.ingest_ts >= t.exchange_ts for t in seen)


@pytest.mark.asyncio
async def test_a_frame_carrying_several_trades_yields_each_one() -> None:
    payload = json.loads(trade_frame())
    payload["events"][0]["trades"] *= 3
    seen: list[Trade] = []
    async with FakeExchange(Send(json.dumps(payload)), Close()) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), seen)
    assert len(seen) == 3


@pytest.mark.asyncio
async def test_the_real_captured_fixture_streams_without_error() -> None:
    seen: list[Trade] = []
    script: list[Action] = [Send(line) for line in load_fixtures("coinbase/trades.jsonl")]
    script.append(Close())
    async with FakeExchange(*script) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), seen)
    assert seen, "the fixture should contain at least one trade"


@pytest.mark.asyncio
async def test_a_mid_stream_close_is_not_retried() -> None:
    # This IS "deliberately fragile", written as an assertion rather than an
    # omission. Phase 2 changes this test when the supervisor arrives.
    async with FakeExchange(Send(trade_frame()), Close()) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), [])

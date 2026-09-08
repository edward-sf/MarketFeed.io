import json

import pytest
from websockets.exceptions import ConnectionClosedOK

from marketfeed.domain import Trade
from marketfeed.errors import ProtocolError, SchemaError
from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase.adapter import CoinbaseAdapter
from tests.fake_exchange import Action, Close, FakeExchange, Send
from tests.sources.coinbase.test_adapter import drain_into, trade_frame


def adapter_for(fake: FakeExchange, *, min_samples: int = 10) -> CoinbaseAdapter:
    return CoinbaseAdapter(
        ["BTC-USD"],
        uri=fake.uri,
        recv_timeout=5.0,
        failures=FailureRateWindow(window_seconds=60.0, ratio=0.5, min_samples=min_samples),
    )


@pytest.mark.asyncio
async def test_one_garbage_frame_is_skipped_and_the_connection_survives() -> None:
    seen: list[Trade] = []
    script: list[Action] = [
        Send(trade_frame()),
        Send("<html>502 Bad Gateway</html>"),
        Send(trade_frame()),
        Close(),
    ]
    async with FakeExchange(*script) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), seen)

    assert len(seen) == 2


@pytest.mark.asyncio
async def test_a_schema_change_kills_the_stream_loudly() -> None:
    script: list[Action] = [Send("<html>502</html>") for _ in range(30)]
    script.append(Close())
    async with FakeExchange(*script) as fake:
        with pytest.raises(SchemaError):
            await drain_into(adapter_for(fake), [])


@pytest.mark.asyncio
async def test_an_unknown_channel_is_tolerated_not_fatal() -> None:
    seen: list[Trade] = []
    unknown = json.dumps({"channel": "candles", "events": []})
    script: list[Action] = [Send(unknown) for _ in range(30)]
    script.extend([Send(trade_frame()), Close()])
    async with FakeExchange(*script) as fake:
        with pytest.raises(ConnectionClosedOK):
            await drain_into(adapter_for(fake), seen)
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_an_error_frame_from_the_exchange_is_fatal() -> None:
    error = json.dumps({"type": "error", "message": "authentication failure"})
    async with FakeExchange(Send(error), Close()) as fake:
        with pytest.raises(ProtocolError, match="authentication failure"):
            await drain_into(adapter_for(fake), [])

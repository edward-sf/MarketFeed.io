import json

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedOK

from tests.fake_exchange import Close, FakeExchange, Send


@pytest.mark.asyncio
async def test_the_harness_records_what_the_client_sent() -> None:
    async with (
        FakeExchange(Close(), expect_subscribe=1) as fake,
        connect(fake.uri) as ws
    ):
        await ws.send('{"type": "subscribe"}')
        await ws.recv()     # ack
        with pytest.raises(ConnectionClosedOK):
            await ws.recv()

    assert json.loads(fake.received[0]) == {"type": "subscribe"}


@pytest.mark.asyncio
async def test_the_harness_replays_frames_verbatim() -> None:
    raw = '{"channel":"market_trades","price":"1.10"}'
    async with (
        FakeExchange(Send(raw), Close(), expect_subscribe=1) as fake,
        connect(fake.uri) as ws
    ):
        await ws.send("{}")
        await ws.recv()
        assert await ws.recv() == raw

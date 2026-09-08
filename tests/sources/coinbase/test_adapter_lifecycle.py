import asyncio
from contextlib import aclosing

import pytest

from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase.adapter import CoinbaseAdapter
from tests.fake_exchange import FakeExchange, Send, Silence
from tests.sources.coinbase.test_adapter import trade_frame


def adapter_for(fake: FakeExchange, *, recv_timeout: float) -> CoinbaseAdapter:
    return CoinbaseAdapter(
        ["BTC-USD"],
        uri=fake.uri,
        recv_timeout=recv_timeout,
        failures=FailureRateWindow(window_seconds=60.0, ratio=0.5, min_samples=10),
    )


@pytest.mark.asyncio
async def test_a_silent_but_open_socket_times_out() -> None:
    """WebSockets answers protocol pings automatically, so the keepalive stays
    happy and the connection is genuinely alive at TCP level. Only the recv
    deadline catches a feed that has simply stopped speaking.
    """
    async with FakeExchange(Silence()) as fake:
        adapter = adapter_for(fake, recv_timeout=0.05)
        with pytest.raises(TimeoutError):
            async with aclosing(adapter.stream()) as trades:
                async for _ in trades:
                    pass


@pytest.mark.asyncio
async def test_cancelling_the_consumer_closes_the_socket() -> None:
    async with FakeExchange(Send(trade_frame()), Silence()) as fake:
        adapter = adapter_for(fake, recv_timeout=30.0)
        first = asyncio.Event()

        async def consume() -> None:
            async with aclosing(adapter.stream()) as trades:
                async for _ in trades:
                    first.set()

        task = asyncio.create_task(consume())
        await asyncio.wait_for(first.wait(), timeout=2.0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The real assertion: cleanup ran. The socket is closed, not leaked
        # until garbage collection.
        await asyncio.wait_for(fake.disconnected.wait(), timeout=2.0)

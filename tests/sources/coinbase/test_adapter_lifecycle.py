import asyncio
from collections.abc import Iterable

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


class ExplodingAdapter(CoinbaseAdapter):
    """Fails after the socket has opened, before __aenter__ can return."""

    def _subscribe_payloads(self) -> Iterable[str]:
        raise RuntimeError("boom after the socket opened")


@pytest.mark.asyncio
async def test_a_failure_during_setup_does_not_leak_the_socket() -> None:
    """Once connect() has succeeded, anything that goes wrong before __aenter__
    returns leaves an open socket that nothing else will ever close. Verified:
    without the guard in __aenter__ this socket genuinely leaks.

    A stalled SERVER cannot trigger this. __aenter__ only sends subscribes and
    never reads the acks, so it returns long before any server-side stall
    matters. The failure has to come from our side of the handshake.
    """
    async with FakeExchange(Silence()) as fake:
        adapter = ExplodingAdapter(
            ["BTC-USD"],
            uri=fake.uri,
            recv_timeout=30.0,
            failures=FailureRateWindow(window_seconds=60.0, ratio=0.5, min_samples=10),
        )
        with pytest.raises(RuntimeError, match="boom"):
            async with adapter as _trades:
                pass

        await asyncio.wait_for(fake.disconnected.wait(), timeout=2.0)


@pytest.mark.asyncio
async def test_a_silent_but_open_socket_times_out() -> None:
    """WebSockets answers protocol pings automatically, so the keepalive stays
    happy and the connection is genuinely alive at TCP level. Only the recv
    deadline catches a feed that has simply stopped speaking.
    """
    async with FakeExchange(Silence()) as fake:
        adapter = adapter_for(fake, recv_timeout=0.05)
        with pytest.raises(TimeoutError):
            async with adapter as trades:
                async for _ in trades:
                    pass


@pytest.mark.asyncio
async def test_cancelling_the_consumer_closes_the_socket() -> None:
    async with FakeExchange(Send(trade_frame()), Silence()) as fake:
        adapter = adapter_for(fake, recv_timeout=30.0)
        first = asyncio.Event()

        async def consume() -> None:
            async with adapter as trades:
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

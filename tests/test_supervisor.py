import asyncio
from collections.abc import Callable
from contextlib import suppress

import pytest
from websockets.exceptions import ConnectionClosedError

from marketfeed.domain import Trade
from marketfeed.supervisor import Supervisor
from tests.doubles import FakeClock, FakeSleep, Idle, StubAdapter, make_trade


class TickingTrade:
    """Advances a FakeClock when the supervisor publishes. Lets a test say
    'this connection stayed up for N seconds' without waiting N seconds."""

    def __init__(self, clock: FakeClock, seconds: float) -> None:
        self._clock = clock
        self._seconds = seconds
        self.count = 0

    def __call__(self, _trade: Trade) -> None:
        self.count += 1
        self._clock.advance(self._seconds)


def collect_into(sink: list[Trade], target: int, done: asyncio.Event) -> Callable[[Trade], None]:
    def publish(trade: Trade) -> None:
        sink.append(trade)
        if len(sink) >= target:
            done.set()

    return publish


async def run_until(supervisor: Supervisor, done: asyncio.Event) -> None:
    """Run the supervisor until `done`, then cancel it.

    The 2s guard is real time but never fires on a passing run. It exists so
    a broken supervisor fails the test of hanging CI.
    """
    task = asyncio.create_task(supervisor.run())
    try:
        await asyncio.wait_for(done.wait(), timeout=2.0)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_trades_reach_the_publish_callback() -> None:
    adapter = StubAdapter("stub", [make_trade(price="1.00"), make_trade(price="2.00"), Idle()])
    seen: list[Trade] = []
    done = asyncio.Event()
    supervisor = Supervisor(
        [adapter], publish=collect_into(seen, 2, done), sleep=FakeSleep(), clock=FakeClock()
    )

    await run_until(supervisor, done)

    assert [str(t.price) for t in seen] == ["1.00", "2.00"]


@pytest.mark.asyncio
async def test_a_transient_failure_reconnects_on_the_expected_schedule() -> None:
    adapter = StubAdapter(
        "stub",
        [ConnectionClosedError(None, None)],
        [ConnectionClosedError(None, None)],
        [ConnectionClosedError(None, None)],
        [make_trade(), Idle()],
    )
    seen: list[Trade] = []
    done = asyncio.Event()
    sleeper = FakeSleep()
    supervisor = Supervisor(
        [adapter],
        publish=collect_into(seen, 1, done),
        sleep=sleeper,
        clock=FakeClock(),
        jitter=lambda d: d,
    )

    await run_until(supervisor, done)

    assert adapter.connections == 4
    assert sleeper.delays == [0.5, 1.0, 2.0]


@pytest.mark.asyncio
async def test_a_stream_that_simply_ends_is_retried() -> None:
    # A generator that runs out is not an error, but it is not a working feed
    # either. It must reconnect rather than silently stop.
    adapter = StubAdapter("stub", [], [make_trade(), Idle()])
    seen: list[Trade] = []
    done = asyncio.Event()
    supervisor = Supervisor(
        [adapter], publish=collect_into(seen, 1, done), sleep=FakeSleep(), clock=FakeClock()
    )

    await run_until(supervisor, done)

    assert adapter.connections == 2


@pytest.mark.asyncio
async def test_a_connection_that_stays_up_resets_the_backoff() -> None:
    clock = FakeClock()
    sleeper = FakeSleep()
    adapter = StubAdapter(
        "stub",
        [ConnectionClosedError(None, None)],
        [make_trade(), ConnectionClosedError(None, None)],
        [make_trade(), Idle()],
    )
    done = asyncio.Event()
    tick = TickingTrade(clock, seconds=40.0)

    def publish(trade: Trade) -> None:
        tick(trade)
        if tick.count >= 2:
            done.set()

    supervisor = Supervisor(
        [adapter],
        publish=publish,
        sleep=sleeper,
        clock=clock,
        jitter=lambda d: d,
        reset_after=30.0,
    )

    await run_until(supervisor, done)

    assert sleeper.delays == [0.5, 0.5]


@pytest.mark.asyncio
async def test_a_flapping_endpoint_does_not_reset_the_backoff() -> None:
    """A socket that accepts, lives a moment, and dies is indistinguishable from a
    healthy one at the instant of connect. Resetting on __aenter__ would pin
    this endpoint at the minimum delay forever.
    """
    clock = FakeClock()
    sleeper = FakeSleep()
    adapter = StubAdapter(
        "stub",
        *([make_trade(), ConnectionClosedError(None, None)] for _ in range(4)),
    )
    done = asyncio.Event()
    tick = TickingTrade(clock, seconds=1.0)

    def publish(trade: Trade) -> None:
        tick(trade)
        if tick.count >= 4:
            done.set()

    supervisor = Supervisor(
        [adapter],
        publish=publish,
        sleep=sleeper,
        clock=clock,
        jitter=lambda d: d,
        reset_after=30.0,
    )

    await run_until(supervisor, done)

    assert sleeper.delays[:4] == [0.5, 1.0, 2.0, 4.0]

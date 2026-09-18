import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass

from marketfeed.backoff import Backoff
from marketfeed.domain import Trade
from marketfeed.faults import Fault, classify
from marketfeed.health import Status
from marketfeed.sources.base import TradeSource

log = logging.getLogger(__name__)


def _full_jitter(delay: float) -> float:
    return random.uniform(0, delay)


@dataclass(slots=True)
class AdapterState:
    """Supervisor-owned connection facts. Touched only at connect and failure,
    never on the per-frame hot path."""

    connected: bool = False
    reconnects: int = 0
    last_error: str | None = None
    status: Status = Status.DOWN   # last REPORTED status, for transition logging


class Supervisor:
    """One supervised task per adapter, under a TaskGroup.

    TaskGroup is unconditionally one_for_all: any child that raises cancels its
    siblings. We get one_for_one because _supervise never lets a transient
    escape - it loops. Permanent faults deliberately DO escape, and
    TaskGroup's built-in behavior then supplies "escalate and die" for free.
    There is no switch here labelled one_for_one; the exception filter is it.
    """

    def __init__(
        self,
        adapters: Sequence[TradeSource],
        *,
        publish: Callable[[Trade], None],
        connect_timeout: float = 10.0,
        base_delay: float = 0.5,
        max_delay: float = 60.0,
        reset_after: float = 30.0,
        degraded_after: float = 5.0,
        down_after: float = 60.0,
        report_interval: float = 10.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[float], float] = _full_jitter
    ) -> None:
        self._adapters = list(adapters)
        self._publish = publish
        self._connect_timeout = connect_timeout
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._reset_after = reset_after
        self._degraded_after = degraded_after
        self._down_after = down_after
        self._report_interval = report_interval
        self._sleep = sleep
        self._clock = clock
        self._jitter = jitter
        self._state = {adapter.name: AdapterState() for adapter in self._adapters}

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for adapter in self._adapters:
                tg.create_task(self._supervise(adapter), name=f"adapter-{adapter.name}")

    async def _supervise(self, adapter: TradeSource) -> None:
        backoff = Backoff(base=self._base_delay, maximum=self._max_delay, jitter=self._jitter)
        state = self._state[adapter.name]
        while True:
            started = self._clock()
            try:
                async with AsyncExitStack() as stack:
                    async with asyncio.timeout(self._connect_timeout):
                        trades = await stack.enter_async_context(adapter)
                    state.connected = True
                    async for trade in trades:
                        self._publish(trade)
                # Falling out here means the stream ended without raising.
                # Not an error, but not a working feed either: reconnect.
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if classify(exc) is Fault.PERMANENT:
                    raise   # kills the TaskGroup, and with it the process
                state.connected = False
                state.reconnects += 1
                state.last_error = repr(exc)
                log.warning("%s failed, will retry: %r", adapter.name, exc)

            if self._clock() - started >= self._reset_after:
                backoff.reset()
            await self._sleep(backoff.next_delay())

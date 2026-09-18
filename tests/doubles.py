import asyncio
import time
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType

from marketfeed.domain import Side, Trade


class FakeClock:
    """A hand-rolled clock beats freezegun here: no dependency, no global
    patching, and the test reads as a timeline."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records what it was asked to wait and returns immediately."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        # Yield, or the retry loop never suspends, a sibling is never
        # scheduled, and the isolation tests spin instead of interleaving.
        await asyncio.sleep(0)


def make_trade(exchange: str = "stub", price: str = "1.00") -> Trade:
    now = datetime.now(UTC)
    return Trade(
        exchange=exchange,
        symbol="BTC-USD",
        price=Decimal(price),
        size=Decimal("0.001"),
        side=Side.BUY,
        exchange_ts=now,
        ingest_ts=now,
    )


@dataclass(frozen=True, slots=True)
class Hang:
    """__aenter__ never returns. Exercises the supervisor's connect timeout."""


@dataclass(frozen=True, slots=True)
class Idle:
    """The stream stays open and produces nothing further."""


type Item = Trade | BaseException | Idle
type Session = Hang | Sequence[Item]


class StubAdapter:
    """A real TradeSource, scripted per connection.

    Not a mock: nothing records calls or asserts on them. It is the same idea
    as FakeExchange - a real implementation that misbehaves on demand - for
    the cases where contorting a socket into producing a KeyError would tell
    you nothing about the supervisor.
    """

    def __init__(
            self,
            name: str,
            *sessions: Session,
            clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.last_frame_at: float | None = None
        self.connections = 0
        self.exits = 0
        self._sessions = list(sessions)
        self._clock = clock

    async def __aenter__(self) -> AsyncGenerator[Trade]:
        session = self._sessions[min(self.connections, len(self._sessions) - 1)]
        self.connections += 1
        if isinstance(session, Hang):
            await asyncio.Event().wait()   # never returns
            raise AssertionError("unreachable: Hang waits forever")
        return self._stream(session)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.exits += 1

    async def _stream(self, session: Sequence[Item]) -> AsyncGenerator[Trade]:
        for item in session:
            if isinstance(item, Idle):
                await asyncio.Event().wait()
            elif isinstance(item, BaseException):
                raise item
            else:
                self.last_frame_at = self._clock()
                yield item

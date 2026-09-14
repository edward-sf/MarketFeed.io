import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable, Iterable, Sequence
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect

from marketfeed.domain import Trade
from marketfeed.errors import MalformedMessageError, SchemaError
from marketfeed.failure_rate import FailureRateWindow

log = logging.getLogger(__name__)


class TradeSource(Protocol):
    """What the supervisor actually depends on.

    Deliberately narrower than Adapter: the supervisor needs a name, a
    staleness stamp, and a connection lifecycle. It does not need to know that
    a socket exists. That is what lets the supervisor be tested with no socket.
    """

    name: str
    last_frame_at: float | None

    async def __aenter__(self) -> AsyncGenerator[Trade]: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


class Adapter(ABC):
    """One exchange's connection lifecycle. Owns no reconnection policy.

    Everything uniform across exchanges lives here: the socket, the receive
    deadline, parse-failure accounting, the staleness stamp. Everything that
    varies (the URI, the subscribe payloads, the message union, symbol
    translation, and the match) lives in the subclass.
    """

    name: str

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        uri: str,
        recv_timeout: float,
        failures: FailureRateWindow,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._symbols = list(symbols)
        self._uri = uri
        self._recv_timeout = recv_timeout
        self._failures = failures
        self._clock = clock
        self._stack: AsyncExitStack | None = None
        self.last_frame_at: float | None = None

    async def __aenter__(self) -> AsyncGenerator[Trade]:
        stack = AsyncExitStack()
        try:
            ws = await stack.enter_async_context(connect(self._uri))
            for payload in self._subscribe_payloads():
                await ws.send(payload)
            trades = self._stream(ws)
            stack.push_async_callback(trades.aclose)
        except BaseException:
            await stack.aclose()
            raise
        self._stack = stack
        return trades

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._stack is not None
        stack, self._stack = self._stack, None
        await stack.aclose()

    async def _stream(self, ws: ClientConnection) -> AsyncGenerator[Trade]:
        while True:
            async with asyncio.timeout(self._recv_timeout):
                raw = await ws.recv()

            self.last_frame_at = self._clock()

            try:
                trades = self._handle(raw, ingest_ts=datetime.now(UTC))
            except MalformedMessageError as exc:
                self._failures.record_failure()
                if self._failures.tripped():
                    raise SchemaError(f"{self.name} parse failure rate exceeded") from exc
                log.warning("%s: skipped malformed frame: %s", self.name, exc)
                continue

            self._failures.record_success()
            for trade in trades:
                yield trade

    @abstractmethod
    def _subscribe_payloads(self) -> Iterable[str]:
        """Frames to send immediately after the socket opens."""

    @abstractmethod
    def _handle(self, raw: str | bytes, ingest_ts: datetime) -> tuple[Trade, ...]:
        """Parse one frame.

        Raises MalformedMessageError for a frame this exchange's parser cannot
        read, or ProtocolError for a frame that says we are not going to work.
        Returns the trades it carried; empty for heartbeats and acks.
        """

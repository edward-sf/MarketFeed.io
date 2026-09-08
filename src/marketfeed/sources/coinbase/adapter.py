import asyncio
import logging
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime
from typing import assert_never

from websockets.asyncio.client import connect

from marketfeed.domain import Trade
from marketfeed.errors import MalformedMessageError, ProtocolError, SchemaError
from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase.messages import (
    ErrorFrame,
    Heartbeat,
    Subscribed,
    TradeBatch,
    UnknownFrame,
)
from marketfeed.sources.coinbase.parse import parse_message, subscribe_payloads

log = logging.getLogger(__name__)


class CoinbaseAdapter:
    name = "coinbase"

    def __init__(
            self,
            symbols: Sequence[str],
            *,
            uri: str,
            recv_timeout: float,
            failures: FailureRateWindow,
    ) -> None:
        self._symbols = list(symbols)
        self._uri = uri
        self._recv_timeout = recv_timeout
        self._failures = failures

    async def stream(self) -> AsyncGenerator[Trade]:
        async with connect(self._uri) as ws:
            for payload in subscribe_payloads(self._symbols):
                await ws.send(payload)

            while True:
                async with asyncio.timeout(self._recv_timeout):
                    raw = await ws.recv()

                try:
                    msg = parse_message(raw, ingest_ts=datetime.now(UTC))
                except MalformedMessageError as exc:
                    self._failures.record_failure()
                    if self._failures.tripped():
                        raise SchemaError("coinbase parse failure rate exceeded") from exc
                    log.warning("skipped malformed frame: %s", exc)
                    continue

                self._failures.record_success()

                match msg:
                    case TradeBatch(trades=trades):
                        for trade in trades:
                            yield trade
                    case Subscribed(channels=()):
                        # A bogus product_id gets a normal ack subscribing to
                        # nothing. Fail here with a useful message rather than
                        # letting recv_timeout fire ten seconds later.
                        raise ProtocolError(
                            f"coinbase subscribed to nothing for {self._symbols}"
                        )
                    case Heartbeat() | Subscribed():
                        pass
                    case ErrorFrame(reason=reason):
                        raise ProtocolError(reason)
                    case UnknownFrame(frame_type=frame_type):
                        log.warning("unrecognized frame type %r", frame_type)
                    case _ as unreachable:
                        assert_never(unreachable)

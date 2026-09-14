import logging
from collections.abc import Iterable
from datetime import datetime
from typing import assert_never

from marketfeed.domain import Trade
from marketfeed.errors import ProtocolError
from marketfeed.sources.base import Adapter
from marketfeed.sources.coinbase.messages import (
    ErrorFrame,
    Heartbeat,
    Subscribed,
    TradeBatch,
    UnknownFrame,
)
from marketfeed.sources.coinbase.parse import parse_message, subscribe_payloads

log = logging.getLogger(__name__)


class CoinbaseAdapter(Adapter):
    name = "coinbase"

    def _subscribe_payloads(self) -> Iterable[str]:
        return subscribe_payloads(self._symbols)

    def _handle(self, raw: str | bytes, ingest_ts: datetime) -> tuple[Trade, ...]:
        msg = parse_message(raw, ingest_ts=ingest_ts)
        match msg:
            case TradeBatch(trades=trades):
                return trades
            case Subscribed(channels=()):
                raise ProtocolError(f"coinbase subscribed to nothing for {self._symbols}")
            case Heartbeat() | Subscribed():
                return ()
            case ErrorFrame(reason=reason):
                raise ProtocolError(reason)
            case UnknownFrame(frame_type=frame_type):
                log.warning("unrecognized frame type %r", frame_type)
                return ()
            case _ as unreachable:
                assert_never(unreachable)

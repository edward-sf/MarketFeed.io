from dataclasses import dataclass

from marketfeed.domain import Trade


@dataclass(frozen=True, slots=True)
class TradeBatch:
    """One frame's worth of trades. Coinbase batches; a batch of one is normal."""

    trades: tuple[Trade, ...]


@dataclass(frozen=True, slots=True)
class Subscribed:
    """Cumulative ack. Empty `channels` means the subscribe matched nothing."""

    channels: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Heartbeat:
    sequence: int | None


@dataclass(frozen=True, slots=True)
class ErrorFrame:
    reason: str


@dataclass(frozen=True, slots=True)
class UnknownFrame:
    frame_type: str


type CoinbaseMessage = TradeBatch | Subscribed | Heartbeat | ErrorFrame | UnknownFrame

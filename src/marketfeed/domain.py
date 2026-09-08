from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class Trade:
    """One executed trade, normalized across exchanges."""

    exchange: str  # "coinbase" | "kraken"
    symbol: str  # canonical "BTC-USD", never "XBT/USD"
    price: Decimal  # parsed from the exchange's string, never via a float
    size: Decimal
    side: Side  # aggressor side - Coinbase reports the maker
    exchange_ts: datetime  # tz-aware UTC, as claimed by the exchange
    ingest_ts: datetime  # tz-aware UTC, when we received it
    sequence: int | None = None  # frame-level sequence_num, for gap detection

import json
from datetime import datetime
from typing import Any
from collections.abc import Sequence

from marketfeed.errors import MalformedMessageError
from marketfeed.sources.coinbase.messages import (
    CoinbaseMessage,
    ErrorFrame,
    Heartbeat,
    Subscribed,
    UnknownFrame,
)


TRADES_CHANNEL = "market_trades"
HEARTBEATS_CHANNEL = "heartbeats"


# Coinbase's product form is already canonical; these exist so the seam is in
# place before Kraken's XBT/USD arrives in Phase 2.
def to_canonical(product_id: str) -> str:
    return product_id.strip().upper()


def to_exchange(symbol: str) -> str:
    return symbol.strip().upper()


def subscribe_payloads(symbols: Sequence[str]) -> list[str]:
    products = [to_exchange(s) for s in symbols]
    return [
        json.dumps({
            "type": "subscribe",
            "product_ids": products,
            "channel": channel,
        }) for channel in (TRADES_CHANNEL, HEARTBEATS_CHANNEL)
    ]


def parse_message(raw: str | bytes, *, ingest_ts: datetime) -> CoinbaseMessage:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MalformedMessageError(f"not JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise MalformedMessageError(f"expected a JSON object, got {type(payload).__name__}")

    # Error frames are keyed on `type` and carry no `channel` at all, so they
    # must be recognized before the channel dispatch.
    if payload.get("type") == "error":
        return ErrorFrame(reason=str(payload.get("message", "unspecified")))

    channel = payload.get("channel")
    if not isinstance(channel, str):
        raise MalformedMessageError("frame has neither 'channel' nor an error 'type'")

    match channel:
        case "subscriptions":
            return Subscribed(channels=_subscribed_channels(payload))
        case "heartbeats":
            return Heartbeat(sequence=_optional_int(payload.get("sequence_num")))
        case _:
            return UnknownFrame(frame_type=channel)


def _subscribed_channels(payload: dict[str, Any]) -> tuple[str, ...]:
    events = payload.get("events")
    if not isinstance(events, list) or not events:
        return ()
    first = events[0]
    if not isinstance(first, dict):
        return ()
    subs = first.get("subscriptions")
    if not isinstance(subs, dict):
        return ()
    return tuple(str(channel) for channel in sorted(subs))


def _optional_int(value: Any) -> int | None:
    if not isinstance(value, int | str):
        return None
    try:
        return int(value)
    except ValueError:
        return None

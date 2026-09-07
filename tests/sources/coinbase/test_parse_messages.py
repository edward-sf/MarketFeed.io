import json
from datetime import UTC, datetime

import pytest

from marketfeed.errors import MalformedMessageError
from marketfeed.sources.coinbase.messages import (
    ErrorFrame,
    Heartbeat,
    Subscribed,
    UnknownFrame,
)
from marketfeed.sources.coinbase.parse import parse_message

INGEST = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def parse(payload: object) -> object:
    return parse_message(json.dumps(payload), ingest_ts=INGEST)


def test_an_acknowledgement_reports_which_channels_are_subscribed() -> None:
    # Observed shape: the ack is cumulative, listing the full current set.
    msg = parse(
        {
            "channel": "subscriptions",
            "events": [
                {"subscriptions": {"heartbeats": ["heartbeats"], "market_trades": ["BTC-USD"]}}
            ],
        }
    )
    assert msg == Subscribed(channels=("heartbeats", "market_trades"))


def test_an_empty_acknowledgment_is_reported_as_such() -> None:
    """Coinbase's answer to a bogus product_id: a normal ack subscribing to
    nothing. Task 8 turns this into a ProtocolError rather than waiting for
    recv_timeout to fire ten seconds later with a useless message.
    """
    msg = parse({"channel": "subscriptions", "events": [{"subscriptions": {}}]})
    assert msg == Subscribed(channels=())


def test_a_heartbeat_carries_its_sequence() -> None:
    msg = parse({"channel": "heartbeats", "sequence_num": 41})
    assert msg == Heartbeat(sequence=41)


def test_an_error_frame_carries_its_reason() -> None:
    # Observed shape: error frames are keyed on `type` and carry NO `channel`.
    msg = parse({"type": "error", "message": "authentication failure"})
    assert msg == ErrorFrame(reason="authentication failure")


def test_an_unrecognized_channel_is_unknown_not_malformed() -> None:
    # A new channel appearing is a weak schema signal, NOT a parse failure.
    msg = parse({"channel": "candles", "events": []})
    assert msg == UnknownFrame(frame_type="candles")


def test_input_that_is_not_json_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse_message("<html>502 Bad Gateway</html>", ingest_ts=INGEST)


def test_json_that_is_not_an_object_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse_message("[1, 2, 3]", ingest_ts=INGEST)


def test_a_frame_with_neither_channel_nor_error_type_is_malformed() -> None:
    with pytest.raises(MalformedMessageError):
        parse({"sequence_num": 3})


def test_the_parser_is_pure() -> None:
    """The boundary the spec defends: most tests must not need and event loop.
    If this fails, I/O has leaked into the parsing layer.
    """
    from marketfeed.sources.coinbase import parse as parse_module

    assert "asyncio" not in vars(parse_module)

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidStatus
from websockets.http11 import Response

from marketfeed.errors import MalformedMessageError, ProtocolError, SchemaError
from marketfeed.faults import Fault, classify


def rejected_with(status: int) -> InvalidStatus:
    return InvalidStatus(Response(status, "", Headers()))


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError(),
        ConnectionClosedOK(None, None),
        ConnectionClosedError(None, None),
        OSError("connection refused"),
        rejected_with(502),
        rejected_with(429),
    ],
)
def test_network_faults_are_transient(exc: BaseException) -> None:
    assert classify(exc) is Fault.TRANSIENT


@pytest.mark.parametrize(
    "exc",
    [
        SchemaError("parse failure rate exceeded"),
        ProtocolError("subscribed to nothing"),
        MalformedMessageError("not json"),
        rejected_with(401),
        rejected_with(403),
    ],
)
def test_our_own_errors_and_hard_rejections_are_permanent(exc: BaseException) -> None:
    assert classify(exc) is Fault.PERMANENT


def test_an_unrecognized_exception_is_permanent() -> None:
    assert classify(KeyError("events")) is Fault.PERMANENT

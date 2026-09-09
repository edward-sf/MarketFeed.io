from enum import StrEnum

from websockets.exceptions import ConnectionClosed, InvalidStatus

from marketfeed.errors import MarketFeedError

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class Fault(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"


def classify(exc: BaseException) -> Fault:
    """Decide whether the supervisor retries this or escalates it.

    The default is PERMANENT. An exception we did not name is a bug, not a
    network blip, and it should die with a traceback rather than spin.
    """

    match exc:
        case MarketFeedError():
            return Fault.PERMANENT
        case InvalidStatus() as rejected:
            if rejected.response.status_code in _RETRYABLE_STATUS:
                return Fault.TRANSIENT
            return Fault.PERMANENT
        case TimeoutError() | ConnectionClosed() | OSError():
            return Fault.TRANSIENT
        case _:
            return Fault.PERMANENT

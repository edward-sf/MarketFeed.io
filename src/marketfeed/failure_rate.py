import time
from collections import deque
from collections.abc import Callable


class FailureRateWindow:
    """Rolling parse-failure rate over a time window.

    Distinguishes one bad message (count and skip) from every message being
    unparseable (the exchange changed its schema; die loudly).
    """

    def __init__(
        self,
        *,
        window_seconds: float,
        ratio: float,
        min_samples: int,
        # time.monotonic for time window,
        # avoiding timezone changes and other clock inconsistencies
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._window_seconds = window_seconds
        self._ratio = ratio
        self._min_samples = min_samples
        self._clock = clock
        self._events: deque[tuple[float, bool]] = deque()

    def record_success(self) -> None:
        self._record(failed=False)

    def record_failure(self) -> None:
        self._record(failed=True)

    def tripped(self) -> bool:
        self._evict()
        if len(self._events) < self._min_samples:
            return False
        failures = sum(1 for _, failed in self._events if failed)
        return failures / len(self._events) >= self._ratio

    def _record(self, *, failed: bool) -> None:
        self._events.append((self._clock(), failed))
        self._evict()

    def _evict(self) -> None:
        cutoff = self._clock() - self._window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

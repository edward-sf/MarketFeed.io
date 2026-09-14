import random
from collections.abc import Callable


def _full_jitter(delay: float) -> float:
    return random.uniform(0, delay)


class Backoff:
    """Exponential backoff with full jitter.

    Un-jittered backoff synchronizes clients: when an exchange restarts a load
    balancer, every client reconnects on the same schedule, amplifying the
    outage and earning rate limits. Sampling uniformly below the current delay
    spreads the herd out.

    Jitter is injected so tests can assert an exact schedule and so the
    policy is configuration in production rather than a literal.
    """

    def __init__(
        self,
        *,
        base: float,
        maximum: float,
        jitter: Callable[[float], float] = _full_jitter,
    ) -> None:
        self._base = base
        self._maximum = maximum
        self._jitter = jitter
        self._current = base

    def reset(self) -> None:
        self._current = self._base

    def next_delay(self) -> float:
        delay = self._jitter(self._current)
        self._current = min(self._current * 2, self._maximum)
        return delay

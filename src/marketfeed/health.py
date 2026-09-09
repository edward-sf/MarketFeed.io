from dataclasses import dataclass
from enum import StrEnum


class Status(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class ExchangeHealth:
    """One exchange's condition at a moment. Phase 3's Snapshot embeds this;
    Phase 4's /health serializes it."""

    name: str
    status: Status
    stale_for: float | None
    reconnects: int
    last_error: str | None


def evaluate(
    *,
    connected: bool,
    stale_for: float | None,
    degraded_after: float,
    down_after: float,
) -> Status:
    """Three states, not a boolean.
    
    DEGRADED means 'stale, and we are working on it.' DOWN means 'stale long
    enough that you should stop assuming it is coming back'. A binary
    green/red collapses those two, and the difference is the whole content of
    'isolated but not hidden'.
    """
    if stale_for is None or stale_for >= down_after:
        return Status.DOWN
    if connected and stale_for < degraded_after:
        return Status.HEALTHY
    return Status.DEGRADED

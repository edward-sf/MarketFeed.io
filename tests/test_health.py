import pytest

from marketfeed.health import Status, evaluate


def status(connected: bool, stale_for: float | None) -> Status:
    return evaluate(connected=connected, stale_for=stale_for, degraded_after=5.0, down_after=60.0)


def test_an_exchange_that_has_never_delivered_is_down() -> None:
    # None is not zero. "No data yet" must not read as "data one moment ago".
    assert status(connected=True, stale_for=None) is Status.DOWN


def test_connected_and_flowing_is_health() -> None:
    assert status(connected=True, stale_for=0.2) is Status.HEALTHY


def test_disconnected_but_recent_is_degraded() -> None:
    assert status(connected=False, stale_for=2.0) is Status.DEGRADED


def test_connected_but_stale_is_degraded() -> None:
    assert status(connected=True, stale_for=7.0) is Status.DEGRADED


@pytest.mark.parametrize("connected", [True, False])
def test_stale_past_the_down_threshold_is_down_either_way(connected: bool) -> None:
    # Past down_after, whether the socket happens to be open is not the
    # interesting fact. The data is old.
    assert status(connected=connected, stale_for=61.0) is Status.DOWN


def test_the_down_boundary_is_inclusive() -> None:
    assert status(connected=True, stale_for=60.0) is Status.DOWN
    assert status(connected=True, stale_for=59.9) is Status.DEGRADED

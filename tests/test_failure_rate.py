from marketfeed.failure_rate import FailureRateWindow


class FakeClock:
    """A hand-rolled clock beats freezegun here: no dependency, no global
    patching, and the test reads as a timeline. We don't care to handle a
    dependency used for a handful of test cases and this is easy to build.
    """

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def window(clock: FakeClock) -> FailureRateWindow:
    return FailureRateWindow(
        window_seconds=60.0,
        ratio=0.5,
        min_samples=10,
        clock=clock
    )


def test_a_fresh_window_has_not_tripped() -> None:
    assert not window(FakeClock()).tripped()


def test_a_handful_of_failures_at_startup_does_not_trip_it() -> None:
    # Two failures out of two is a 100% failure rate. Without a minimum sample
    # size, the first malformed frame after a restart would kill the process.
    clock = FakeClock()
    w = window(clock)
    w.record_failure()
    w.record_failure()
    assert not w.tripped()


def test_a_healthy_stream_does_not_trip_it() -> None:
    clock = FakeClock()
    w = window(clock)
    for _ in range(99):
        w.record_success()
    w.record_failure()
    assert not w.tripped()


def test_a_sustained_majority_of_failures_trips_it() -> None:
    clock = FakeClock()
    w = window(clock)
    for _ in range(20):
        w.record_failure()
    assert w.tripped()


def test_old_failures_fall_out_of_the_window() -> None:
    clock = FakeClock()
    w = window(clock)
    for _ in range(20):
        w.record_failure()
    assert w.tripped()

    clock.advance(61.0)
    for _ in range(10):
        w.record_success()
    assert not w.tripped()


def test_an_idle_window_does_not_trip() -> None:
    # No traffic at all is not the same as failing traffic. A quiet market must
    # not look like a schema change.
    clock = FakeClock()
    w = window(clock)
    for _ in range(20):
        w.record_failure()
    clock.advance(3600.0)
    assert not w.tripped()

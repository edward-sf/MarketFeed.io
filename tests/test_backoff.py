from marketfeed.backoff import Backoff


def test_the_schedule_doubles_and_then_caps() -> None:
    b = Backoff(base=0.5, maximum=4.0, jitter=lambda d: d)
    assert [b.next_delay() for _ in range(6)] == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]


def test_reset_returns_to_the_base_delay() -> None:
    b = Backoff(base=0.5, maximum=60.0, jitter=lambda d: d)
    b.next_delay()
    b.next_delay()
    b.reset()
    assert b.next_delay() == 0.5


def test_full_jitter_samples_between_zero_and_the_undecayed_delay() -> None:
    # Full jitter sleeps uniform(0, current). The UNDECAYED value is what
    # doubles; the sampled value is only what we wait. Getting this backwards
    # gives a schedule that decays toward zero.
    b = Backoff(base=1.0, maximum=8.0)
    for ceiling in (1.0, 2.0, 4.0, 8.0):
        delay = b.next_delay()
        assert 0.0 <= delay <= ceiling


def test_jitter_actually_varies() -> None:
    # Guards against a "jitter" that quietly returns its argument. Sixteen
    # draws from a continuous distribution collapsing to one value is
    # impossible in practice.
    b = Backoff(base=100.0, maximum=100.0)
    assert len({b.next_delay() for _ in range(16)}) > 1

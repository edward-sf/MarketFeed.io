from contextlib import aclosing

import pytest

from marketfeed.config import Settings
from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase import CoinbaseAdapter


@pytest.mark.live
@pytest.mark.asyncio
async def test_the_live_feed_still_matches_our_parser() -> None:
    """Manual schema check. Excluded from CI: exchange maintenance must never
    turn our build red."""
    settings = Settings()
    adapter = CoinbaseAdapter(
        ["BTC-USD"],
        uri=settings.coinbase_uri,
        recv_timeout=30.0,
        failures=FailureRateWindow(window_seconds=60.0, ratio=0.5, min_samples=20),
    )
    async with aclosing(adapter.stream()) as trades:
        async for trade in trades:
            assert trade.symbol == "BTC-USD"
            assert trade.exchange_ts.tzinfo is not None
            break

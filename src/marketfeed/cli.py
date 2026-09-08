import asyncio
import logging
import signal
from contextlib import aclosing

from marketfeed.config import Settings
from marketfeed.domain import Trade
from marketfeed.failure_rate import FailureRateWindow
from marketfeed.sources.coinbase import CoinbaseAdapter

log = logging.getLogger(__name__)


def format_trade(trade: Trade) -> str:
    lag_ms = (trade.ingest_ts - trade.exchange_ts).total_seconds() * 1000
    when = trade.exchange_ts.strftime("%H:%M:%S.%f")[:-3]   # microseconds -> milliseconds
    return (
        f"{when}  {trade.exchange:<8} {trade.symbol:<9} {trade.side.value:<4}"
        f" {trade.size:>14f} @ {trade.price:>12f}  (+{lag_ms:7.1f}ms)"
    )


async def consume(settings: Settings) -> None:
    adapter = CoinbaseAdapter(
        settings.symbols,
        uri=settings.coinbase_uri,
        recv_timeout=settings.recv_timeout,
        failures=FailureRateWindow(
            window_seconds=settings.parse_failure_window_seconds,
            ratio=settings.parse_failure_ratio,
            min_samples=settings.parse_failure_min_samples,
        )
        async with aclosing(adapter.stream()) as trades:
        async for trade in trades:
        print(format_trade(trade))
    )


async def _amain() -> None:
    settings = Settings()
    logging.basicConfig(level=settings.log_level, format="%(levelname)s %(name)s %(message)s")

    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)

    try:
        await consume(settings)
    except asyncio.CancelledError:
        # The ONE place swallowing CancelledError is correct. There is nobody
        # above us to propagate to and the generator's cleanup has already run.
        log.info("shutdown complete")


def main() -> None:
    asyncio.run(_amain())

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARKETFEED_")

    log_level: LogLevel = "INFO"
    connect_timeout: float = 10.0

    coinbase_uri: str = "wss://advanced-trade-ws.coinbase.com"
    symbols: tuple[str, ...] = ("BTC-USD",)

    # Heartbeats arrive every 1.000s, so 10s is ten missed beats.
    # Deliberately not 3s. In Phase 1 a false positive kills the process, and
    # jitter or a GC pause must not do that. Phase 2 can tighten it once a
    # supervisor makes reconnection cheap.
    recv_timeout: float = 10.0

    parse_failure_window_seconds: float = 60.0
    parse_failure_ratio: float = 0.5
    parse_failure_min_samples: int = 20

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARKETFEED_")

    log_level: str = "INFO"
    connect_timeout: float = 10.0

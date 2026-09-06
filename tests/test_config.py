import pytest

from marketfeed.config import Settings


def test_settings_read_values_from_the_environment(
        monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MARKETFEED_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MARKETFEED_CONNECT_TIMEOUT", "5.0")

    settings = Settings()

    assert settings.log_level == "DEBUG"
    assert settings.connect_timeout == 5.0

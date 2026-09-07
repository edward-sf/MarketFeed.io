class MarketFeedError(Exception):
    """Base for every error this package raises deliberately."""


class MalformedMessageError(MarketFeedError):
    """One frame could not be parsed. Counted and skipped; never fatal on its own."""


class SchemaError(MarketFeedError):
    """The parse-failure rate says the exchange changed its schema. Fatal."""


class ProtocolError(MarketFeedError):
    """The exchange rejected us - bad subscription, bad channel. Fatal."""

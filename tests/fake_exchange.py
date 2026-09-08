import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self, assert_never

from websockets.asyncio.server import Server, ServerConnection, serve

FIXTURES = Path(__file__).parent / "fixtures"

# Non-empty on purpose: an ack with no channels means "subscribed to nothing",
# which the adapter treats as fatal. Tests wanting that failure send their own.
SUBSCRIPTION_ACK = json.dumps(
    {
        "channel": "subscriptions",
        "sequence_num": 0,
        "events": [{"subscriptions": {"market_trades": ["BTC-USD"]}}],
    }
)


@dataclass(frozen=True, slots=True)
class Send:
    payload: str


@dataclass(frozen=True, slots=True)
class Close:
    code: int = 1000


@dataclass(frozen=True, slots=True)
class Silence:
    """Hold the socket open and send nothing, until the client goes away."""


type Action = Send | Close | Silence


def load_fixtures(name: str) -> list[str]:
    """Raw lines, unmodified. Never round-trip these through json."""
    return (FIXTURES / name).read_text().splitlines()


class FakeExchange:
    """A real WebSocket server that replays a script and misbehaves on demand."""

    def __init__(self, *script: Action, expect_subscribe: int = 2) -> None:
        self._script = script
        self._expect_subscribe = expect_subscribe
        self.received: list[str] = []
        self.disconnected = asyncio.Event()
        self._server: Server | None = None

    async def __aenter__(self) -> Self:
        # Port 0: the OS picks a free port. No collisions, no flaky CI.
        self._server = await serve(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    @property
    def uri(self) -> str:
        assert self._server is not None
        host, port = self._server.sockets[0].getsockname()[:2]
        return f"ws://{host}:{port}"

    async def _handle(self, ws: ServerConnection) -> None:
        try:
            for _ in range(self._expect_subscribe):
                self.received.append(str(await ws.recv()))
                await ws.send(SUBSCRIPTION_ACK)

            for action in self._script:
                match action:
                    case Send(payload=payload):
                        await ws.send(payload)
                    case Close(code=code):
                        await ws.close(code)
                        return
                    case Silence():
                        await ws.wait_closed()
                    case _ as unreachable:
                        assert_never(unreachable)
        finally:
            self.disconnected.set()

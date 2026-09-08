"""Throwaway capture tool. Untested by design; not importable from the package

Usage: uv run python scripts/capture.py BTC-USD 120 > out.jsonl
"""

import asyncio
import json
import sys

import websockets

URI = "wss://advanced-trade-ws.coinbase.com"


async def capture(product_id: str, seconds: float) -> None:
    async with websockets.connect(URI) as ws:
        for channel in ("market_trades", "heartbeats"):
            await ws.send(
                json.dumps({"type": "subscribe", "product_ids": [product_id], "channel": channel})
            )
        try:
            async with asyncio.timeout(seconds):
                async for raw in ws:
                    print(raw, flush=True)
        except TimeoutError:
            pass


if __name__ == "__main__":
    asyncio.run(capture(sys.argv[1], float(sys.argv[2])))

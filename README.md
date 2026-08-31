# MarketFeed.io
A real-time financial data ingestion service utilizing Python's asynchronous I/O to stream high-frequency pricing data.

**Domains:** `Market Data`, `Infrastructure`

## Tech Stack

- **Python (`asyncio`, `aiohttp`)**
- **Supabase Realtime**
- **Cloudflare Workers**

## Architecture

Uses Python's `asyncio` to maintain persistent WebSocket connections to public financial exchanges (like crypto or stock APIs). It formats the live pricing data and broadcasts it directly into Supabase Realtime channels, allowing frontend clients to subscribe to live price updates.

# MarketFeed.io

A real-time crypto market data ingestion service built on Python's `asyncio`. It holds persistent WebSocket connections to multiple exchanges, normalizes their divergent message formats into a single domain model, and fans that stream out to consumers with independent backpressure policies.

**Domains:** `Market Data`, `Infrastructure`

> **Status: in development — Phase 1 of 7.** The architecture below is designed and specified; implementation is underway. See the [roadmap](#roadmap) for what exists today.

---

## Architecture

The organising principle: **one internal event stream, several independent consumers, each with its own backpressure policy.** Consumers disagree about what a message is worth — the conflator may discard a superseded price because a human eye can't use it; the archive may not lose data silently, because a hole in an archive is invisible corruption.

```
  ┌──────────────┐   ┌──────────────┐
  │ Coinbase     │   │ Kraken       │      adapters: one per exchange,
  │ adapter      │   │ adapter      │      each yields normalized Trades
  └──────┬───────┘   └──────┬───────┘
         └────────┬─────────┘
                  ▼
          ┌───────────────┐
          │  Supervisor   │              lifecycle: spawn, restart, backoff,
          └───────┬───────┘              fault classification, shutdown
                  ▼
          ┌───────────────┐
          │   Event hub   │              fan-out; each consumer gets its
          └───┬───────┬───┘              OWN bounded queue
      ┌───────┘       └────────┐
      ▼                        ▼
┌──────────────┐      ┌──────────────────────┐
│  Conflator   │      │  Sinks               │
│  → WS server │      │  Postgres / R2       │
│  MAY DROP    │      │  MAY NOT DROP        │
│  (superseded │      │  SILENTLY            │
│   data)      │      │                      │
└──────┬───────┘      └──────────────────────┘
       ▼
  Dashboard (Cloudflare Pages)
```

Three properties the design is built around:

- **Adapters isolate exchange idiosyncrasy.** Kraken says `XBT`, Coinbase says `BTC`; they disagree about handshakes, heartbeats, and trade representation. Adding a third exchange must mean writing one file and touching nothing else.
- **Failures are isolated but never hidden.** A dead exchange doesn't take down its siblings (OTP's `one_for_one`), but staleness surfaces in the snapshot, the dashboard, and the health endpoint. Faults are classified: transient faults retry with jittered backoff; permanent ones — auth failures, a schema change breaking the parser — escalate and kill the process rather than retry forever behind a green status light.
- **Output rate is decoupled from input rate.** Conflation is a requirement, not an optimisation. Drop counts are shipped to the dashboard so backpressure is visible rather than theoretical.

## Tech stack

| What | Why |
| ---- | --- |
| **Python 3.12+ (`asyncio`)** | `TaskGroup` and `asyncio.timeout()` make several failure modes structurally impossible rather than merely discouraged |
| **`websockets`** | Purpose-built for the protocol, and serves as well as connects — exchange clients and the dashboard server from one dependency |
| **Supabase Postgres** | Warm storage for second-resolution OHLCV bars, so charts render on page load |
| **Cloudflare R2** | Cold archive: compressed hourly objects, with explicit gap records where data was shed |
| **Cloudflare Pages** | Static dashboard hosting |
| **Fly.io** | The ingester is a long-lived stateful process; it needs a host that won't sleep it |

## Roadmap

- [x] **Phase 0** — Foundations: tooling, strict typing, CI
- [x] **Phase 1** — One socket, one exchange, stdout. The adapter is complete and well-behaved; the supervisor that would restart cheaply it doesn't exist yet.
- [ ] **Phase 2** — Resilience: supervisor, jittered backoff, watchdog, second exchange
- [ ] **Phase 3** — Hub, conflation, backpressure
- [ ] **Phase 4** — Server, dashboard, deploy ⭐ *first public demo*
- [ ] **Phase 5** — Warm storage: OHLCV bars to Postgres
- [ ] **Phase 6** — Cold archive: compressed objects to R2
- [ ] **Phase 7** — Soak testing, hardening, write-up

## Design documentation

- [Specs and Architecture documentation](docs/SPECS.md)
  — the full design: domain model, supervision strategy, backpressure policies, testing approach

Development is test-driven throughout. Rather than mocking the network, tests will run against
a fake exchange server — a real local WebSocket server — that reproduces the failures worth
testing: mid-stream disconnects, malformed frames, half-open sockets that accept and then go
silent forever, and schema changes that break the parser.

## Getting Started

**Requirements:** Python 3.12+ and [`uv`](https://docs.astral.sh/uv/). The project pins a `uv`-managed interpreter, so `uv` will fetch the right Python for you.

```bash
uv sync
uv run marketfeed
```

You should see live BTC-USD trades within a second or two:

```
16:53:25.664  coinbase BTC-USD   sell     0.00000004 @     78760.34  (+16039.1ms)
16:53:25.896  coinbase BTC-USD   buy      0.00235332 @     78760.35  (+15807.1ms)
16:53:25.953  coinbase BTC-USD   sell     0.00000621 @     78760.34  (+15749.5ms)
```


The trailing figure is `ingest_ts - exchange_ts` and represents the end-to-end latency between the exchange timestamping a trade and this process receiving it. A drift in that number is the earliest signal of clock skew or a degrading connection.

`Ctrl-C` shuts down cleanly. The socket is closed through the generator's cleanup path rather than dropped.

### Configuration

Every setting is an environment variable prefixed `MARKETFEED_`.

| Variable | Default | Meaning |
| -------- | ------- | ------- |
| `MARKETFEED_SYMBOLS` | `["BTC-USD"]` | Products to subscribe to. **JSON**, not comma-separated |
| `MARKETFEED_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `MARKETFEED_RECV_TIMEOUT` | `10.0` | Seconds without a frame before the connection is treated as dead. Coinbase heartbeats arrive every 1.0s, so this is ten missed beats |
| `MARKETFEED_PARSE_FAILURE_RATIO` | `0.5` | Failure rate over the window that means the schema changed |
| `MARKETFEED_PARSE_FAILURE_WINDOW_SECONDS` | `60.0` | Length of the failure window |
| `MARKETFEED_PARSE_FAILURE_MIN_SAMPLES` | `20` | Frames required before the ratio is trusted (i.e., 5/5 failed frames aren't fatal if the next 15 are successful) |

`pydantic-settings` parses complex types as JSON, so a list needs JSON syntax:

```bash
MARKETFEED_SYMBOLS='["ETH-USD","BTC-USD"]' uv run marketfeed
```

A comma-separated string is rejected outright rather than silently misparsed.

Trades go to **stdout**; logs go to **stderr**. `uv run marketfeed > trades.txt` captures the data while warnings stay visible.

### Tests

```bash
uv run pytest
```

Runs in well under a second. Nothing sleeps a real duration; timeouts and clocks are injected and the WebSocket tests run against a local fake exchange rather than the network.

```bash
uv run pytest -m live
```

Connects to the real Coinbase feed and checks the wire format still matches the parser. It is excluded from the default run and from CI, so exchange maintenance can't turn the build red.

Run it manually when trades stop appearing or a parser change is suspected.

### Capturing fresh fixtures

```bash
uv run python scripts/capture.py BTC-USD 120 > /tmp/coinbase-raw.jsonl
```

Dumps two minutes of raw frames to JSONL. `tests/fixtures/coinbase/trades.jsonl` is a trimmed capture, committed verbatim. The tests replay exactly the bytes the exchange sent, so re-serializing a fixture through JSON encoder would defeat the purpose.

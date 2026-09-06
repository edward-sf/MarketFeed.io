# MarketFeed.io

A real-time crypto market data ingestion service built on Python's `asyncio`. It holds persistent WebSocket connections to multiple exchanges, normalizes their divergent message formats into a single domain model, and fans that stream out to consumers with independent backpressure policies.

**Domains:** `Market Data`, `Infrastructure`

> **Status: in development — Phase 0 of 7.** The architecture below is designed and specified; implementation is underway. See the [roadmap](#roadmap) for what exists today.

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
- [ ] **Phase 1** — One socket, one exchange, stdout (deliberately fragile: no reconnection)
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

## Getting started

Not yet — there's nothing to run until Phase 1. Setup instructions land with the first adapter.

# MarketFeed.io - Architecture & Roadmap Design

**Date:** 2026-09-05
**Status:** In review
**Author**: Edward Simpson-Fitzgibbon

---

## 1. Purpose

MarketFeed.io is a real-time crypto market data ingestion service. It maintains persistent WebSocket connections to multiple exchanges, normalizes their divergent message formats into a single domain model, and fans that stream out to several consumers with independent backpressure policies: a live dashboard, a warm time-series store, and a cold archive.

### Learning Objectives

As a learning portfolio project, this project does not prioritize production utility or market disruption. It is focused on developing understanding, not delivery speed. The finished artifact is a portfolio piece: a publicly hosted live dashboard plus a short video explaining the design decisions.

The following are the primary learning objectives for this project:

1. **Async Python in depth**: the `asyncio` concurrency model, WebSocket lifecycle, structured concurrency, cancellation safety, backpressure, graceful shutdown, and testing async code.

2. **End-to-end system design**: service boundaries, data contracts, failure modes, deployment, and observability.

The following are explicitly *not* goals of this project's current scope.

- **Market-data domain depth**: Order books, cross-venue arbitrage, and tick-level correctness are not learning objectives. Finance is the *vehicle*. It supplies a fast, messy, genuinely real-time stream. Domain features are included only where they create an interesting concurrency problem.

- **Production operations depth**: Monitoring, alerting, and deployment are designed honestly but kept deliberately small with no Prometheus/Grafana stack.

---

## 2. Data sources

At release, MarketFeed.io will support **Coinbase** and **Kraken** exchanges. Both offer public WebSocket feeds requiring no API key, running 24/7. This keeps the demo live whenever a visitor arrives, and provides message rates high enough to make backpressure real rather than conceptual.

Of course, the two exchanges disagree about subscription mesage shape, channel naming, heartbeats, sequence numbers, and trade representation. One example is the ticker used for Bitcoin: Kraken uses `XBT` whereas Coinbase uses `BTC`. These disagreements create a normalization boundary, which we handle through the use of exchange-tailored `Adapter`s.

Expanding the pool of supported exchanges will be as simple as adding additional `Adapter`s. The application lifecycle lives downstream with the `Supervisor`. We'll discuss more about this relationship in the next section.

## 3. Architecture

### Organizing Principle

One internal event stream, several independent consumers, each with its own backpressure policy.

Consumers disagree about what a message is worth; the conflator may discard a superseded price because a human eye cannot use it. The archive must *not* lose data sliently, because a hole in an archive is invisible corruption and undermines trust.

```
  ┌──────────────┐   ┌──────────────┐
  │ Coinbase     │   │ Kraken       │      adapters: one per exchange,
  │ adapter      │   │ adapter      │      each yields normalized Trades
  └──────┬───────┘   └──────┬───────┘
         └────────┬─────────┘
                  ▼
          ┌───────────────┐
          │  Supervisor   │              owns lifecycle: spawn, restart,
          └───────┬───────┘              backoff, fault classification, shutdown
                  ▼
          ┌───────────────┐
          │   Event hub   │              fan-out; each consumer gets its
          └───┬───────┬───┘              OWN bounded queue
      ┌───────┘       └────────┐
      ▼                        ▼
┌──────────────┐      ┌──────────────────────┐
│  Conflator   │      │  Sinks (Phases 5–6)  │
│  → WS server │      │  Postgres / R2       │
│  MAY DROP    │      │  MAY NOT DROP        │
│  (superseded │      │  SILENTLY            │
│   data)      │      │  (sheds with gap     │
│              │      │   records; see §6)   │
└──────┬───────┘      └──────────────────────┘
       ▼
  Dashboard (Cloudflare Pages)
```

### Components

#### Adapters (`sources/coinbase.py`, `sources/kraken.py`)

```
async def stream(self, symbols: list[str]) -> AsyncIterator[Trade]: ...
```

Each `Adapter` owns exactly one exchange's idiosyncrasies: subscribe handshake, heartbeat format, field names, and symbol translation in both directions. It does **not** own reconnection, which lives in the `Supervisor`. This keeps the policy uniform, testable, and not duplicated per exchange.

##### Boundary Test

Adding a third exchange must require writing one new file implementing `stream()` and touching nothing else. Any proposed change that breaks this property is a design violation and should be reframed.

#### `Supervisor`

One task per `Adapter` under an `asyncio.TaskGroup`. Restarts failed adapters with jittered exponential backoff, classifies faults, propagates cancellation on shutdown.

#### Event Hub

`Adapter`s publish `Trade`s to which consumers subscribe. Each consumer holds its own bounded queue. The hub never blocks a producer because one consumer is slow.

#### Conflator -> WS Server

Collapses $N$ trades/sec into one snapshot per fixed interval, holding the latest state per `(symbol, exchange)`. Explicitly permitted to drop superseded data; drops are counted and reported. Idles when no clients are connected.

#### Sinks

Batched Postgres bar writer (warm) and R2 archiver (cold).

#### Dashboard

Static assets on Cloudflare Pages, connecting directly to the WS server.

---

## 4. Domain Model

```
# marketfeed/domain.py
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class Trade:
    """One executed trade, normalized across exchanges."""
    exchange: str                   # "coinbase" | "kraken"
    symbol: str                     # canonical "BTC-USD". Never "XBT/USD"
    price: Decimal                  # parsed from the exchange's string, never via a float
    size: Decimal
    side: Side                      # aggressor side
    exchange_ts: datetime           # tz-aware UTC, as claimed by the exchange
    ingest_ts: datetime             # tz-aware UTC, when we received it
    sequence: int | None = None     # exchange-provided, for gap detection
```

### Rationale

The following are intentional choices that influence the design:

- **`frozen=True` is a concurrency decision.** The hub hands the same object to multiple consumers. `asyncio` being single-threaded does not prevent data races. `await` is precisely where another task may run, so a coroutine that reads a field, awaits, and reads again can observe two values. Immutability removes the bug class by construction. `slots=True` is a memory concern.

- **`Decimal`, parsed from the exchange's JSON string.** Both exchanges send prices as strings (`"64213.57"`) deliberately. `float()` is lossy and discards precision (`64213.56999999...`). At these message rates the performance difference is irrelevant and the habit is worth forming.

- **Two timestamps, both timezone-aware UTC.** `exchange_ts` is claimed; `ingest_ts` is observed. Their delta is the end-to-end latency metric, and a shift in it reveals clock skew or a degrading connection before anything visibly breaks. Naive datetimes are strictly prohibited in this domain.

- **Canonical symbols are translated inside the `Adapter`**, in both directions. Nothing downstream learns that Kraken calls Bitcoin `XBT`, for example.

### Snapshot

The conflator emits a `Snapshot` containing latest `Quote` per `(symbol, exchange)`, a monotonic sequence number, **per-exchange health and staleness**, and stats (trades seen, dropped, max lag, conflation ratio). Shipping drop counts to the dashboard makes backpressure visible rather than theoretical.

### Serialization

Domain models know nothing about JSON. Serialization lives in a separate `wire.py`. `Snapshot` keys on `tuple[str, str]`, which is not JSON-expressible. This is deliberate so transport concerns stay outside the domain and a future JSON->msgpack change only needs to update one file.

### Validation Strategy

Parse at the boundary, once, so the interior can trust its inputs. `parse_trade()` raises `MalformedMessage`, caught in the adapter loop, counted, and logged with sampling. **One bad message must never kill a connection, and must never be silently swallowed.**

### Pydantic

I've chosen to not use Pydantic for the domain model in early development. The domain model, as it exists currently, is constructed only from validated inputs and re-validation is pure overhead where immutability and slots are wanted. Parsers are hand-written in early phases (Phases 1-2) so their failure modes are learned first-hand. Pydantic *may* be adopted later as a deliberate refactor (e.g., its tagged unions are a natural fit for multiplexed message dispatch). `pydantic-settings` is adopted immediately to open the door for later configuration.

---

## 5. Concurrency Design

**Python 3.12+ is required.** `asyncio.TaskGroup` and `asyncio.timeout()` make several failure modes structurally impossible rather than merely discouraged.

`websockets` for both exchange clients and the dashboard server. It is purpose-built for the protocol (better close handshakes, ping/pong, backpressure primitives) and serves as well as connects, giving both sides from one dependency, with HTTP health checks via its `process_request` hook. This revises the original README's `aiohttp` assumption.

### Structured Concurrency

```
async def run(self) -> None:
    async with asyncio.TaskGroup() as tg:
        for adapter in self.adapters:
            tg.create_task(self._supervise(adapter))
```

`asyncio.create_task()` returns a task the loop holds only a **weak** reference to and dropping your reference allows the garbage collector to collect a running task mid-flight, silently. `TaskGroup` binds every task to a scope.

### Supervision Strategy

`TaskGroup`'s built-in behavior is OTP's `one_for_all` (i.e., any child raising cancels all siblings). **We deliberately implement `one_for_one**; an exchange failing must not tear down the others.

```
async def _supervise(self, adapter: Adapter) -> None:
    delay = 0.5
    while True:
        try:
            async with asyncio.timeout(self.connect_timeout):
                await adapter.connect()
            delay = 0.5         # reset only after real success
            await adapter.run(self.hub)
        except asyncio.CancelledError:
            raise               # shutdown, never swallow
        except TransientError:
            log.exception("adapter %s failed", adapter.name)
            self.metrics.reconnects.inc(adapter.name)
        await asyncio.sleep(random.uniform(0, delay))   # full jitter
        delay = min(delay * 2, 60.0)
```

- **`CancelledError` is re-raised, never swallowed.** It inherits from `BaseException` (3.8+), so `except Exception` will not catch it. Swallowing it means ignoring shutdown and being `SIGKILL`ed mid-write.

- **Full jitter, not bare exponential backoff.** When an exchange restarts a load balancer, every client reconnects at once. Un-jittered backoff produces synchronized retry waves that amplify the outage and earn rate limits.

- **Reset the delay only after a genuine connection**, never after a socket that opened and died immediately. Otherwise, a flapping endpoint pins retries at the minimum forever.

### Fault Classification

Isolation is not unconditional. The exception filter is narrow and the fault class determines the policy.

| **Fault Class** | **Examples** | **Policy** |
| ----------- | -------- | ------ |
| **Transient** | Network blip, 502, timeout, exchange restart | Retry with jittered backoff, isolated |
| **Permanent** | Auth failure, bad config, schema change breaking the parser | Do not retry. Escalate and die. |
| **Systemic** | All adapters failing, OOM, disk full | Die. Let the platform restart. |

`AuthError`, `ConfigError`, and `SchemaError` propagate and terminate the process. This requires the parser to distinguish *one* malformed message (count, skip) from *every* message being unparseable (the API changed; deny loudly). A rolling parse-failure rate crossing a threshold is the trigger.

Rationale for not simply isolating everything is that partial availability can be worse than none where outputs are cross-venue. Recovery code is the least-tested code in a system. Infinite retry hides outages and some failures will never succeed on retry.

### Isolation Must Not Hide Degradation

`Snapshot` carries per-exchange health and staleness. The dashboard greys out a stale exchange with its staleness duration. `/health` reports **degraded**, not a binary green/red. Isolating a failure is only defensible if the failure remains impossible to ignore.

### Known Traps We Design Against

- **A blocking call freezes every connection.** One thread, one loop. Detected with `loop.set_debug(True)` with warning on callbacks >100ms. Genuinely blocking work goes in `asyncio.to_thread()`.

- **Half-open sockets hang forever.** The server dies without sending FIN. `await ws.recv()` waits indefinitely with nothing raised. This is the most common production failure in WebSocket feeds. Mitigation through wrap `recv()` in `asyncio.timeout()` sized slightly above the exchange's heartbeat interval and treat a timeout as a dead connection.

- **Shutdown is a deadline, not a request.** Platforms send `SIGTERM`, wait a grace period, then `SIGKILL`. Sinks flush under a timeout; an undeadlined drain is a slower `SIGKILL` with unplanned data loss.

---

## 6. Backpressure and fan-out

A producer *never* awaits a consumer.

```
def publish(self, trade: Trade) -> None:    # NOT async
    for sub in self._subs:
        try:
            sub.queue.put_nowait(trade)
        except asyncio.QueueFull:
            sub.on_overflow(trade)          # per-subscriber policy
```

`await queue.put()` would couple an exchange connection's health to a sink's disk latency.

The synchronous signature is a design property. `publish` contains no `await`, therefore no suspension point and the event loop cannot interleave another task mid-fan-out. No consumer observes a half-delivered broadcast. This is the core `asyncio` mental model (`await` is where other code gets to run, and nowhere else) which is also why locks are rarely needed here and why a single blocking call is catastrophic.

### Queue Sizing

`asyncio.Queue()` defaults to `maxsize=0`, meaning it's **unbounded**. This can cause memory leaks in disguise and 256MB instances are fatal. Every queue in our design is bounded, forcing the question of what we do when the queue is full.

Queues are sized in *time*, not items. A 1,000-slot queue at 500 msg/sec is two seconds of buffer.

#### Per-consumer Policies

- **The conflator is a mailbox, not a queue.** It has two tasks:\
    (1) An ingest task awaiting trades and updating a `dict[(symbol, exchange)] -> Quote`\
    (2) An emit task waking on an interval to snapshot it.\
    No lock is needed because the dict update contains no `await`. The dict must be copied before emitting. Awaiting while iterating allows the ingest task to mutate it underneath.

- **WS clients are a one-slot mailbox each, latest wins.** For state-like data a queue is the wrong structure; a slow client wants the *current* snapshot, not a backlog of stale ones. Replacement semantics turn backpressure into graceful degradation. Each client gets its **own writer task**. Iterating clients with an inline `await ws.send()` would stall everyone behind the slowest.

- **Sinks are bounded, batched, and loud.** No queue size rescues a consumer that is persistently slower than its producer. **Queue absorb bursts, not sustained overload.** The real fix is making the sink fast via batching (accumulate until $N$ records or $T$ seconds, then write once), turning thousands of inserts into a handful of `COPY`s and thousands of `PUT`s into one compressed object.

- **Sink overflow policy is 'shed and shout'.** When a sink falls behind despite batching drop, count, mark health degraded, and record the gap explicitly so the archive is *known* incomplete rather than quietly wrong. Local disk spill and ingest-side backpressure were considered and rejected. The archive is not the product for this phase of development and stalling ingest to protect a file nobody is reading would kill the live demo.

#### Metrics

Four numbers exported and displayed:

1. **queue depth** per consumer
2. **drops** per consumer
3. **age of oldest queued item**, the true lag signal
4. **conflation ratio** (i.e., trades in vs snapshots out)

---

## 7. Testing Strategy

Test-driven development is our north star and should be used in every stage of development. Two rules:

1. **Red before green**: a test that has never failed has not proven it catches anything.

2. **Test at the boundary you're defending**: assert "a malformed message is counted and the connection survives", not "`_parse_field` was called twice".

### Keep Logic Pure

Async is an I/O concern. Logic stays synchronous and pure. Parsers, conflation updates, and bar aggregation are sync functions requiring no event loop, no fixtures, and no timing. **~80% of tests never touch asyncio.** Failure to achieve this indicates logic tangled into I/O. Testability is a boundary check.

### The Fake Exchange Harness

Because `websockets` serves as well as connects, tests start a real local WebSocket server replaying recorded fixtures, not a mock. This is what makes TDD viable on I/O-heavy code without mock-heavy tests that verify only the mocks. We'll build this in Phase 1, before the first adapter.

```
# tests/conftest.py
class FakeExchange:
    """Real WS server that replays fixtures and misbehaves on demand."""

    async def __aenter__(self):
        # port 0 -> OS picks a free port; no collisions, no flaky CI
        self._server = await websockets.serve(self._handle, "127.0.0.1", 0)
        self.uri = f"ws://127.0.0.1:{self._server.sockets[0].getsockname()[1]}
        return self
```

| **Fake exchange behavior** | **What it proves** |
| -------------------------- | ------------------ |
| Closes mid-stream | Supervisor reconnects with backoff |
| Sends one garbage message | Counted and skipped; connection survives |
| Accepts, then goes silent forever | Watchdog fires (half-open socket) |
| Sends 10k messages instantly | Backpressure engages; drops counted, memory bounded |
| Changes field names | Parse-failure rate crosses threshold; process dies loudly |

### Fixtures

Capture several minutes of live Coinbase and Kraken traffic to JSONL in Phase 1 and commit a trimmed sample. This provides honest test data containing real-world oddities, and an offline replay mode for development and video recording without depending on live markets.

### Async testing rules

- **Never sleep real durations in tests.** Backoff timings are injected, not hardcoded, which makes them configuration in production too.

- **Test cancellation explicitly.** Cancel a running adapter; assert the socket closed and the queue drained. Shutdown-only cleanup paths are otherwise first exercised in production.

- **Do not test the exchange.** Live-connection tests sit behind a marker, excluded from CI. They are valuable as a manual schema check but must not make CI fail during exchange maintenance.

### CI

GitHub Actions running `ruff` (lint+format), `mypy --strict`, and `pytest`. `mypy --strict` is retained despite the friction. It catches `Decimal | None` errors and forces honesty about optionality.

### Observability

Deliberately minimal in this stage of development. Structured JSON logs to stdout (captured by the platform; high-frequency events sampled). `/health` reporting per-exchange status. Metrics ride inside the snapshot rather than a separate endpoint. The dashboard already receives snapshots so monitoring and demo share one mechanism.

---

## 8. Infrastructure

### Portfolio-wide Strategy

As we're supporting a portfolio on a budget, our infrastructure options carry some constraints.

One Supabase project, one Postgres schema per app. Pro's $10 compute credit covers one Micro instance. Adding another Supabase project costs another $10/month. Dedicating a project per portfolio piece would quickly balloon our costs.

**MarketFeed.io** uses a `marketfeed` schema with a dedicated, scoped DB role. The accepted cost is shared blast radius, mitigated by schema isolation, per-app roles, and migrations committed in each repo.

For frontend hosting, we're using Cloudflare Pages. Cloudflare R2 holds cold archives in one bucket under per-project prefixes. Cloudflare Workers' free tier covers read-only API edges.

We'll use Fly.io for compute - a small always-on shared-CPU machine (~$2-4/mo), free TLS, Docker deploy, no sysadmin overhead. If four or more projects later need persistent processes, we'll consolidate onto a single ~$5/mo VPS running Docker Compose.

### Frontend

For this stage of development, we'll use plain TypeScript + Vite + uPlot without a framework. uPlot is small and purpose-built for streaming time series. A React setup would consume time that is meant to go into async work.

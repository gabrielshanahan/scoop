# Release Notes

All notable changes to Scoop are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

## v0.2.10 — 2026-05-01

### Added

- `PostgresMessageQueue.pauseTicks()` / `resumeTicks()` — temporarily pauses the scheduled-tick path on every active subscription (including the internal `sleep-handler`). Intended for test fixtures that need to TRUNCATE Scoop's tables without racing live ticks; without it, TRUNCATE's `AccessExclusiveLock` deadlocks with the tick's `AccessShareLock` from `SELECT FOR UPDATE SKIP LOCKED`.

### Changed

- `PostgresMessageQueue` now implements `AutoCloseable`. The Quarkus `ScoopProducer` registers a CDI `@Disposes` method that calls `messageQueue.close()` on bean destroy, which stops the internal `sleep-handler` subscription before the surrounding `DataSource` tears down.

### Fixed

- `Subscription.close()` now blocks until in-flight ticks have actually drained: `PeriodicTick.close()` calls `executor.awaitTermination(...)` after `shutdown()`, with a `shutdownNow()` fallback if the soft timeout expires. Previously close() returned immediately and ticks kept running on their daemon thread; combined with Quarkus tearing down ArC and Agroal in parallel with `@PreDestroy`, this produced unbounded `Error in when ticking` / `pool is closed` / `ArC container not initialized` log spam during application shutdown. Tick-failure logs are also demoted to DEBUG once the queue is shutting down, silencing the racy log lines that fire during the @PreDestroy / Agroal teardown window.

## v0.2.9 — 2026-04-23

### Added

- `instances` parameter on `PostgresMessageQueue.subscribe(...)` — spins up N independent workers for a saga within a single JVM, each with its own serialised tick loop and `DistributedCoroutineIdentifier` instance UUID. Workers compete via Postgres `FOR UPDATE SKIP LOCKED`, the same mechanism used for multi-service horizontal scaling. Defaults to 1 (unchanged behaviour).
- `PostgresMessageQueue.requiredConnectionCount` property — minimum number of database connections Scoop may hold concurrently for its registered workers' event loops (equal to the sum of `instances` across all subscriptions, including the internal `sleep-handler`). Intended as an assertion target in integration tests: `assertTrue(poolMaxSize >= messageQueue.requiredConnectionCount)`.

### Changed

- `EventLoop.tickPeriodically` now returns a `PeriodicTick` handle (superseding the previous `AutoCloseable` return). `PeriodicTick.trigger()` queues an ad-hoc tick on the same single-thread executor that drives the schedule, so scheduled ticks and LISTEN/NOTIFY-driven wake-ups are serialised per saga identifier. Triggers that arrive while another tick is pending or running are coalesced — at most one tick is ever queued on the executor — so bursts of LISTEN/NOTIFY traffic cannot grow the queue unboundedly.

### Fixed

- LISTEN/NOTIFY callbacks for a subscribed saga no longer run on virtual threads concurrently with the scheduled tick — they are now funneled through the saga's single-thread tick executor. Previously a single worker could process multiple messages in parallel via two tick entry points, which defeated the "one `DistributedCoroutineIdentifier` = one serial worker" model; parallelism now comes exclusively from `instances > 1`.

## v0.2.8 — 2026-04-06

### Fixed

- `CooperationContext` serialization now preserves `@JsonTypeInfo` discriminators for collection elements inside `MappedElement`s, fixing `InvalidTypeIdException` on round-trip of polymorphic collections

## v0.2.7 — 2026-04-05

### Changed

- Overhaul logging: most INFO-level logs downgraded to DEBUG, keeping INFO for lifecycle events only
- Add strategic DEBUG logging across EventLoop, Capabilities, continuations, and repositories for execution tracing
- Fix pre-existing detekt LongMethod violations in StubHandlerBlockingTest

## v0.2.6 — 2026-04-05

### Added

- Configurable event loop tick interval — `tickInterval` parameter on `PostgresMessageQueue` and `Scoop.create()`, with `scoop.tick-interval-ms` application property for Quarkus (default: 50ms)

## v0.2.5 — 2026-04-03

### Fixed

- `PgSubscriberTopicNotifier` callbacks now dispatch on virtual threads instead of the Vert.x event loop, preventing blocking when tick processing takes longer than expected

## v0.2.4 — 2026-04-02

### Fixed

- `CooperationContext.readAsString()` now correctly re-escapes decoded JSON strings during round-trip serialization

## v0.2.3 — 2026-04-02

### Fixed

- `CooperationContext.readAsString()` no longer silently drops `null` values during round-trip serialization

## v0.2.2 — 2026-03-30

### Changed

- Bump Quarkus 3.24.5 → 3.32.3 (with `cachePreparedStatements` API compatibility fix)
- Bump PostgreSQL driver 42.7.5 → 42.7.10
- Bump SLF4J 2.0.16 → 2.0.17

## v0.2.1 — 2026-03-20

### Added

- Type-safe topic identifiers via `Topic<P>` — replaces raw string topic names with typed objects
- `Handler<P>` — binds a handler name, topic, and implementation together as a single type-safe object
- `Action<I, O>` — a handler that produces a return value, with built-in `storeActionResult` convenience
- `ActionTopic<P>` and `ActionInput<P>` — topic and payload wrapper for actions that return values
- `VariableName` — sealed base for type-safe return value variable identifiers
- `Handler.saga(eventLoopStrategy) {}` extension — builds a saga using the handler's class name automatically
- `messageQueue.subscribe(handler)` extension — subscribes a handler to its topic in one call
- `getReturnValues` and `getReturnValue` now accept `Handler<*>` instead of raw strings, providing compile-time safety when retrieving child return values

## v0.2.0 — 2026-02-27

### Added

- Loops and control flow via `NextStep` (`Repeat`, `GoTo`, `Continue`)
- Step iteration counter (`stepIteration` parameter)
- Named steps with `GoTo` targeting

### Changed

- `saga {}` builder step lambdas now receive a `stepIteration: Int` parameter
- Event loop strategy can be configured per saga

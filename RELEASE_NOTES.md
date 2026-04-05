# Release Notes

All notable changes to Scoop are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

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

package io.github.gabrielshanahan.scoop.coroutine

/**
 * Handle for a periodic tick loop started by
 * [EventLoop.tickPeriodically][io.github.gabrielshanahan.scoop.coroutine.EventLoop.tickPeriodically].
 *
 * Backed by a single-thread executor, so every tick for a given saga identifier — whether produced
 * by the periodic schedule or by an explicit [trigger] — is serialised. This is what guarantees
 * that a single
 * [DistributedCoroutineIdentifier][io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier]
 * never runs overlapping continuations: parallelism comes from registering more *instances* of the
 * same saga (see
 * [PostgresMessageQueue.subscribe][io.github.gabrielshanahan.scoop.messaging.PostgresMessageQueue.subscribe]),
 * never from multiple entry points into one identifier.
 */
interface PeriodicTick : AutoCloseable {
    /**
     * Ask the loop to run a tick as soon as it's idle.
     *
     * Intended for push-based wake-ups (e.g. Postgres LISTEN/NOTIFY callbacks). The triggered tick
     * is guaranteed to be executed sequentially with respect to scheduled ticks — there is at most
     * one tick running per saga identifier at any given moment.
     *
     * ## Coalescing
     *
     * Triggers that arrive while another tick is already running or pending are silently dropped —
     * they do not pile up on the executor's queue. This is safe: every tick drains all currently
     * ready work before it returns (via the event loop's inner `whileISaySo` loop, which opens a
     * fresh transaction per iteration), so the in-flight tick will observe any messages committed
     * during its run. In the rare case a wake-up is dropped just as the previous tick is finishing,
     * the next scheduled tick (within the configured interval) acts as a safety net — the same
     * safety net that keeps Scoop correct without LISTEN/NOTIFY at all.
     *
     * Calls after [close] are silently ignored.
     */
    fun trigger()
}

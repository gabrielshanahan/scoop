package io.github.gabrielshanahan.scoop.coroutine

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Decides, per worker, whether a given [tick][EventLoop.tick] should run the
 * [reconciliation][io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.MessageEventRepository.startContinuationsForCoroutine]
 * step (the EMITTED→SEEN / ROLLBACK_EMITTED→ROLLING_BACK anti-joins over `message_event`).
 *
 * ## Why this exists
 *
 * Reconciliation only has work to do when a new `EMITTED` or `ROLLBACK_EMITTED` row appeared on
 * this worker's topic. Every such row is accompanied by a Postgres `NOTIFY` (the `message` insert
 * trigger for `EMITTED`, and the `ROLLBACK_EMITTED` insert trigger added in `V5`), which is bridged
 * to [PeriodicTick.trigger][PeriodicTick.trigger]. On a purely periodic "safety-net" tick with no
 * intervening notification, the anti-joins are guaranteed empty — running them is pure waste. With
 * N idle subscribed topics that waste is ~2 `message_event` scans per topic per tick, forever.
 *
 * This gate turns reconciliation from "every tick" into "only when a notification says there might
 * be work, plus a rare safety sweep".
 *
 * ## Drain, don't reconcile-once
 *
 * A single notification can require **several** reconcile passes to fully drain, because sibling
 * handlers reconciling messages under the **same** parent contend on that parent's `SEEN` row via
 * `FOR UPDATE SKIP LOCKED`: on a contended pass the loser inserts nothing and must retry once the
 * winner releases the lock. Reconciling exactly once per notification would strand the loser until
 * the safety-net sweep (tens of seconds). So once woken, the gate keeps reconciling every tick
 * until it observes [QUIET_TICKS] **consecutive** passes that inserted nothing — any productive
 * pass resets the counter, so an arbitrary fan-out of contending siblings drains in order. After
 * the tail goes quiet the worker stops reconciling (idle topics cost ~zero) until the next
 * notification or safety sweep.
 *
 * ## Safety net
 *
 * Independently of notifications, a reconcile is forced at least every [forceReconcileEvery]. This
 * preserves Scoop's "correct without LISTEN/NOTIFY at all" guarantee: even if every notification
 * were lost (dropped LISTEN, server crash wiping the async-notify queue, a row skipped under
 * contention for longer than the quiet tail), reconciliation still runs within that interval —
 * correctness never depends on notification delivery, only latency does.
 *
 * ## Concurrency
 *
 * [markDirty] is called from the NOTIFY callback thread (a virtual thread, see
 * [io.github.gabrielshanahan.scoop.messaging.TopicNotifier]); every other method runs on the
 * worker's single-thread tick executor. Only [dirtySignal] crosses threads, so it is the lone
 * [AtomicBoolean]; [quietTicksRemaining] and [nextSweepNanos] are touched solely by the executor
 * thread.
 *
 * The critical ordering is **consume-before-work**: [shouldReconcile] clears the signal *before*
 * the reconcile transaction opens its snapshot, so a notification landing *during* the reconcile
 * re-arms the gate and is honoured on the next tick rather than being silently cleared after the
 * fact (a notification always fires *after* its row commits; the clear happens *before* the
 * subsequent snapshot, so any signal a tick consumes corresponds to a row that tick will see).
 */
class ReconcileGate private constructor(private val intervalNanos: Long, seedNanos: Long) {

    // Set by NOTIFY (any thread), consumed by the executor in shouldReconcile. Starts armed so the
    // first tick after subscribe/startup reconciles, catching messages emitted while this worker
    // was
    // down.
    private val dirtySignal = AtomicBoolean(true)

    // Remaining ticks to keep reconciling before going idle; reset whenever a notification arrives
    // or
    // a reconcile pass is productive. Executor-thread-only.
    private var quietTicksRemaining = QUIET_TICKS

    // Monotonic (wall-clock-independent) instant of the next forced safety-net reconcile.
    @Volatile private var nextSweepNanos: Long = seedNanos

    /**
     * Records that a notification arrived for this worker's topic. Safe to call from any thread.
     */
    fun markDirty() {
        dirtySignal.set(true)
    }

    /**
     * Returns whether this tick should reconcile, atomically consuming the dirty signal. A gate
     * built with a non-positive interval ([ALWAYS]) never gates — it reconciles every tick,
     * preserving the pre-gating behaviour for direct [EventLoop.tick] callers (e.g. tests).
     */
    fun shouldReconcile(nowNanos: Long = System.nanoTime()): Boolean {
        if (intervalNanos <= 0L) return true
        if (dirtySignal.getAndSet(false)) {
            quietTicksRemaining = QUIET_TICKS
        }
        // Subtraction compare is immune to nanoTime() wraparound (its value is meaningful only as a
        // difference).
        return quietTicksRemaining > 0 || nowNanos - nextSweepNanos >= 0L
    }

    /**
     * Records the outcome of a committed reconcile pass and pushes the safety-net timer forward. A
     * pass that inserted rows keeps the drain going (more contending work may remain); a pass that
     * inserted nothing counts towards the quiet tail.
     */
    fun reconcileSucceeded(insertedRows: Int, nowNanos: Long = System.nanoTime()) {
        if (intervalNanos <= 0L) return
        quietTicksRemaining =
            if (insertedRows > 0) QUIET_TICKS else (quietTicksRemaining - 1).coerceAtLeast(0)
        nextSweepNanos = nowNanos + intervalNanos
    }

    /** Keeps the drain armed so a failed/rolled-back reconcile is retried on the next tick. */
    fun reconcileFailed() {
        if (intervalNanos > 0L) {
            quietTicksRemaining = QUIET_TICKS
        }
    }

    companion object {
        /**
         * Consecutive zero-insert reconcile passes required before a woken worker stops
         * reconciling. Sized to absorb transient `FOR UPDATE SKIP LOCKED` contention on a parent
         * SEEN row (each contended sibling resolves within a tick or two); the safety net covers
         * anything longer.
         */
        private const val QUIET_TICKS = 3

        /** A non-gating gate: always reconciles. Default for direct [EventLoop.tick] callers. */
        val ALWAYS = ReconcileGate(0L, 0L)

        /**
         * Builds a per-worker gate that forces a reconcile at least every [forceReconcileEvery].
         * The first sweep is seeded with a random offset in `[0, forceReconcileEvery)` so that many
         * workers booted together do not all sweep on the same cadence (which would produce a
         * synchronized reconcile herd across the fleet).
         */
        fun create(
            forceReconcileEvery: Duration,
            nowNanos: Long = System.nanoTime(),
        ): ReconcileGate {
            val intervalNanos = forceReconcileEvery.toNanos()
            if (intervalNanos <= 0L) return ALWAYS
            @Suppress(
                "InsecureRandom"
            ) // not security-sensitive: only spreads safety sweeps in time
            val jitterNanos = (Math.random() * intervalNanos).toLong()
            return ReconcileGate(intervalNanos, nowNanos + jitterNanos)
        }
    }
}

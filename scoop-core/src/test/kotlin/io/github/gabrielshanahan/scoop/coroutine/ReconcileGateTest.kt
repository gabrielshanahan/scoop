package io.github.gabrielshanahan.scoop.coroutine

import java.time.Duration
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReconcileGateTest {

    private val intervalSeconds = 30L

    // A gate driven by an explicit monotonic clock so the safety-net timing is deterministic.
    private fun gate(startNanos: Long = 0L): ReconcileGate =
        ReconcileGate.create(Duration.ofSeconds(intervalSeconds), nowNanos = startNanos)

    private val Int.secondsAsNanos: Long
        get() = Duration.ofSeconds(this.toLong()).toNanos()

    // Reconcile (consume + run + commit an empty pass) until the gate goes idle, returning how many
    // passes ran. Bounded so a never-quiescing gate fails the test instead of looping forever.
    private fun drainUntilIdle(g: ReconcileGate, nowNanos: Long): Int {
        var passes = 0
        while (g.shouldReconcile(nowNanos)) {
            g.reconcileSucceeded(insertedRows = 0, nowNanos = nowNanos)
            passes++
            check(passes < 100) { "gate never went idle" }
        }
        return passes
    }

    @Test
    fun `ALWAYS always reconciles regardless of state`() {
        val g = ReconcileGate.ALWAYS
        assertTrue(g.shouldReconcile())
        g.reconcileSucceeded(insertedRows = 0)
        assertTrue(g.shouldReconcile(), "ALWAYS never gates")
    }

    @Test
    fun `starts armed so the first tick reconciles, then drains to idle`() {
        val g = gate()
        // Fresh gate is armed; it drains a short quiet tail of empty passes then goes idle.
        val passes = drainUntilIdle(g, nowNanos = 0L)
        assertTrue(passes in 1..10, "should reconcile a small bounded tail, was $passes")
        assertFalse(g.shouldReconcile(nowNanos = 1.secondsAsNanos), "idle after the tail")
    }

    @Test
    fun `markDirty re-arms the drain`() {
        val g = gate()
        drainUntilIdle(g, nowNanos = 0L)
        assertFalse(g.shouldReconcile(nowNanos = 1.secondsAsNanos))
        g.markDirty()
        assertTrue(
            g.shouldReconcile(nowNanos = 1.secondsAsNanos),
            "a notification re-arms the gate",
        )
    }

    @Test
    fun `a productive pass keeps the drain going (handles contending siblings)`() {
        val g = gate()
        drainUntilIdle(g, nowNanos = 0L)
        g.markDirty()
        // First pass after the notification inserts nothing (e.g. it lost a SKIP LOCKED race)...
        assertTrue(g.shouldReconcile(nowNanos = 1.secondsAsNanos))
        g.reconcileSucceeded(insertedRows = 0, nowNanos = 1.secondsAsNanos)
        // ...next pass wins the lock and inserts a row, which must reset the tail so further
        // contending work still gets drained.
        assertTrue(g.shouldReconcile(nowNanos = 1.secondsAsNanos))
        g.reconcileSucceeded(insertedRows = 1, nowNanos = 1.secondsAsNanos)
        assertTrue(
            g.shouldReconcile(nowNanos = 1.secondsAsNanos),
            "productive pass resets the tail",
        )
    }

    @Test
    fun `safety net forces a reconcile when idle`() {
        val g = gate()
        drainUntilIdle(g, nowNanos = 0L)
        assertFalse(g.shouldReconcile(nowNanos = (intervalSeconds - 1).toInt().secondsAsNanos))
        assertTrue(
            g.shouldReconcile(nowNanos = (intervalSeconds + 1).toInt().secondsAsNanos),
            "safety net fires once the interval elapses",
        )
    }

    @Test
    fun `reconcileFailed re-arms so the next tick retries`() {
        val g = gate()
        drainUntilIdle(g, nowNanos = 0L)
        // A notification wakes the gate; its reconcile then fails (throws).
        g.markDirty()
        assertTrue(g.shouldReconcile(nowNanos = 1.secondsAsNanos))
        g.reconcileFailed()
        assertTrue(g.shouldReconcile(nowNanos = 1.secondsAsNanos), "failure re-arms the retry")
    }

    @Test
    fun `a notification landing during a reconcile survives the consume-before-work clear`() {
        val g = gate()
        drainUntilIdle(g, nowNanos = 0L)
        // tick consumes the (absent) signal and finds idle
        assertFalse(g.shouldReconcile(nowNanos = 1.secondsAsNanos))
        // notification lands; even though a hypothetical in-flight pass already cleared its
        // snapshot,
        // the signal is recorded and honoured next tick.
        g.markDirty()
        g.reconcileSucceeded(insertedRows = 0, nowNanos = 1.secondsAsNanos)
        assertTrue(g.shouldReconcile(nowNanos = 2.secondsAsNanos), "late notification is not lost")
    }
}

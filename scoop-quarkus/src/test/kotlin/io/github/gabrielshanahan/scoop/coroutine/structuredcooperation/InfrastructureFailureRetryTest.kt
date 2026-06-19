package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.ScoopInfrastructureException
import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.coroutine.getEventSequence
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Covers the core property that keeps perpetual sagas alive across transient infrastructure faults:
 * a [ScoopInfrastructureException] raised while a step is being processed (Scoop's own bookkeeping
 * failing — e.g. a dead JDBC connection) must NOT roll the saga back. The run is left at its last
 * committed step and retried on a later tick. A plain business exception, by contrast, must still
 * roll back exactly as before.
 *
 * The infrastructure failure is simulated by throwing [ScoopInfrastructureException] from the step
 * body: the event loop funnels both a step throw and Scoop's own bookkeeping throw through the same
 * classification point
 * ([io.github.gabrielshanahan.scoop.coroutine.continuation.BaseCooperationContinuation.resumeCoroutine]),
 * so throwing it from the step exercises exactly that seam.
 */
/** A named business exception so the "still rolls back" test does not throw a generic one. */
private class SimulatedBusinessFailure(message: String) : RuntimeException(message)

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InfrastructureFailureRetryTest : StructuredCooperationTest() {

    @Test
    fun `a ScoopInfrastructureException is retried (not rolled back) and the run eventually commits`() {
        val attempts = AtomicInteger(0)
        val committed = CountDownLatch(1)

        val rootHandler = "root-handler"
        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                    step { _, _ ->
                        val attempt = attempts.incrementAndGet()
                        // Fail the first two attempts the way a dead connection would, then
                        // succeed.
                        if (attempt < 3) {
                            throw ScoopInfrastructureException(
                                RuntimeException("simulated dead connection #$attempt")
                            )
                        }
                        committed.countDown()
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                committed.await(20, TimeUnit.SECONDS),
                "the run should eventually succeed by retrying",
            )
            ciSleep(500)

            // Two transient failures, then a success.
            Assertions.assertEquals(3, attempts.get())

            val types = fluentJdbc.getEventSequence().map { it.first }
            Assertions.assertFalse(
                types.contains("ROLLING_BACK"),
                "an infrastructure failure must never enter ROLLING_BACK: $types",
            )
            Assertions.assertFalse(
                types.contains("ROLLED_BACK"),
                "an infrastructure failure must never roll back: $types",
            )
            Assertions.assertTrue(
                types.contains("COMMITTED"),
                "the run should commit after a successful retry: $types",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Test
    fun `a plain business exception still rolls back and is not retried`() {
        val attempts = AtomicInteger(0)

        val rootHandler = "root-handler"
        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga(rootHandler, handlerRegistry.eventLoopStrategy()) {
                    step { _, _ ->
                        attempts.incrementAndGet()
                        throw SimulatedBusinessFailure("business failure")
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
            while (
                System.nanoTime() < deadline &&
                    fluentJdbc.getEventSequence().none { it.first == "ROLLED_BACK" }
            ) {
                ciSleep(200)
            }
            ciSleep(500)

            val types = fluentJdbc.getEventSequence().map { it.first }
            Assertions.assertTrue(
                types.contains("ROLLING_BACK"),
                "a business failure should enter ROLLING_BACK: $types",
            )
            Assertions.assertTrue(
                types.contains("ROLLED_BACK"),
                "a business failure should roll back: $types",
            )
            // A business failure is not retried — the step ran exactly once.
            Assertions.assertEquals(1, attempts.get())
        } finally {
            rootSubscription.close()
        }
    }
}

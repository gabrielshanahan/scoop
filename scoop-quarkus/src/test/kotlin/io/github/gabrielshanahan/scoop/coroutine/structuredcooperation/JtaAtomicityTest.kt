package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import jakarta.transaction.Transactional
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Proves that a JTA `@Transactional` business write performed inside a saga step is atomic with
 * scoop's own `message_event` writes — i.e. both are part of the one per-step JTA transaction begun
 * by [io.github.gabrielshanahan.scoop.quarkus.JtaTransactionRunner].
 *
 * Before this seam existed, calling `@Transactional` business code from a step threw Agroal's
 * "Deferred enlistment not supported", because scoop's step ran on a non-JTA connection while the
 * business code tried to start its own JTA transaction.
 */
/**
 * Dedicated exception so a step can fail deterministically without tripping detekt's generic-throw
 * rule.
 */
private class SimulatedStepFailure(message: String) : Exception(message)

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JtaAtomicityTest : StructuredCooperationTest() {

    /** Business bean that writes to a business table inside a `@Transactional(REQUIRED)` method. */
    @ApplicationScoped
    open class ProbeWriter(private val dataSource: DataSource) {
        @Transactional
        open fun write(id: String) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("INSERT INTO scoop_jta_probe (id) VALUES (?)").use { ps ->
                    ps.setString(1, id)
                    ps.executeUpdate()
                }
            }
        }
    }

    @Inject lateinit var probeWriter: ProbeWriter

    @Inject lateinit var dataSource: DataSource

    @BeforeEach
    fun createProbeTable() {
        fluentJdbc
            .query()
            .update("CREATE TABLE IF NOT EXISTS scoop_jta_probe (id text PRIMARY KEY)")
            .run()
        fluentJdbc.query().update("TRUNCATE TABLE scoop_jta_probe").run()
    }

    private fun probeRowExists(id: String): Boolean =
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT 1 FROM scoop_jta_probe WHERE id = ?").use { ps ->
                ps.setString(1, id)
                ps.executeQuery().use { rs -> rs.next() }
            }
        }

    private fun eventTypesFor(coroutineName: String): List<String> =
        fluentJdbc
            .query()
            .select(
                "SELECT type FROM message_event WHERE coroutine_name = :name ORDER BY created_at"
            )
            .namedParam("name", coroutineName)
            .listResult { it.getString("type") }

    @Test
    fun `business write inside a step commits atomically with scoop events`() {
        val probeId = "commit-${java.util.UUID.randomUUID()}"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                rootTopic,
                saga("jta-commit-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        // A child launch ensures the step's connection (used by scope.launch)
                        // is the same JTA-enlisted connection as the business write.
                        val childPayload = jsonbHelper.toPGobject(mapOf("from" to "jta"))
                        scope.launch(childTopic, childPayload)
                        probeWriter.write(probeId)
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("jta-commit-child", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Step did not run (deferred-enlistment would prevent it)",
            )
            ciSleep(500)

            // (a) it did NOT throw deferred-enlistment: the step ran and committed
            // (b) the business row is present
            Assertions.assertTrue(
                probeRowExists(probeId),
                "Business write should have committed with the step",
            )
            // (c) scoop's SUSPENDED + COMMITTED events for this saga exist
            val events = eventTypesFor("jta-commit-handler")
            Assertions.assertTrue(
                events.contains("SUSPENDED"),
                "Expected a SUSPENDED event, got $events",
            )
            Assertions.assertTrue(
                events.contains("COMMITTED"),
                "Expected a COMMITTED event, got $events",
            )
        } finally {
            subscription.close()
            childSubscription.close()
        }
    }

    @Test
    fun `business write rolls back together with the step when the step throws`() {
        val probeId = "rollback-${java.util.UUID.randomUUID()}"
        val latch = CountDownLatch(1)

        val subscription =
            messageQueue.subscribe(
                rootTopic,
                saga("jta-rollback-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        invoke = { scope, message ->
                            probeWriter.write(probeId)
                            latch.countDown()
                            throw SimulatedStepFailure("Simulated failure after business write")
                        },
                        rollback = { scope, message, throwable -> },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), "Step did not run")
            ciSleep(500)

            // The business write and scoop's step tx are one unit: since the step threw,
            // the business row must have been rolled back with it.
            Assertions.assertFalse(
                probeRowExists(probeId),
                "Business write should have rolled back with the failed step",
            )
        } finally {
            subscription.close()
        }
    }
}

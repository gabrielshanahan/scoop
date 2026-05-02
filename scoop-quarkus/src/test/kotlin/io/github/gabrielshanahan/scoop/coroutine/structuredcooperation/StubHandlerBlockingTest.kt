package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.NextStep
import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.StandardEventLoopStrategy
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import java.time.OffsetDateTime
import java.util.Collections
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Tests that a saga step properly blocks when a handler is registered in the topology but NOT
 * subscribed to the event loop (a "stub handler").
 *
 * Models the pattern where a handler is declared in the topology purely to satisfy the "who is
 * listening" check, but is never actually subscribed in-process — for example, a human-driven step
 * where a user (or some external system) is expected to write SEEN + COMMITTED events out-of-band.
 * The expectation is that the parent saga does NOT resume until those events appear for the stub
 * handler.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StubHandlerBlockingTest : StructuredCooperationTest() {

    private val stubTopic = "stub-topic"
    private val stubHandlerName = "stub-handler"

    /**
     * Creates an event loop strategy that includes the stub handler in the topology, modelling the
     * human-handler / out-of-band-COMMITTED pattern described in the class kdoc.
     */
    private fun strategyWithStubHandler() =
        StandardEventLoopStrategy(OffsetDateTime.now()) {
            handlerRegistry.listenersByTopic().toMutableMap().apply {
                merge(stubTopic, listOf(stubHandlerName)) { a, b -> a + b }
            }
        }

    private fun createRootSagaWithChildAndStub(
        executionOrder: MutableList<String>,
        loopCounter: AtomicInteger,
        latch: CountDownLatch,
    ) =
        messageQueue.subscribe(
            rootTopic,
            saga("root-handler", strategyWithStubHandler()) {
                step(
                    "loop-step",
                    invoke = { scope, message, iteration ->
                        val count = loopCounter.incrementAndGet()
                        executionOrder.add("loop-iter-$iteration")

                        if (count == 1) {
                            scope.launch(
                                childTopic,
                                jsonbHelper.toPGobject(mapOf("from" to "root")),
                            )
                            scope.launch(
                                stubTopic,
                                jsonbHelper.toPGobject(mapOf("task" to "wait-for-human")),
                            )
                            latch.countDown()
                        }
                        NextStep.Repeat
                    },
                )
            },
        )

    @Test
    fun `repeating step blocks when stub handler has not started`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)
        val latch = CountDownLatch(1)

        val rootSubscription = createRootSagaWithChildAndStub(executionOrder, loopCounter, latch)

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> executionOrder.add("child-handler-done") }
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("initial" to "true")),
                )
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "First iteration did not complete",
            )
            ciSleep(2000)

            Assertions.assertEquals(
                listOf("loop-iter-0", "child-handler-done"),
                executionOrder.filter { it != "child-handler-done" || executionOrder.contains(it) },
            )

            val loopIterations = executionOrder.filter { it.startsWith("loop-iter-") }
            Assertions.assertEquals(
                1,
                loopIterations.size,
                "Parent saga should execute only once when stub handler hasn't started. " +
                    "Got: $executionOrder",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    private fun externallyCompleteStubHandler() {
        fluentJdbc.transactional { connection ->
            val stubMessageData =
                fluentJdbc
                    .query()
                    .select(
                        """
                        SELECT m.id as message_id, me.cooperation_lineage
                        FROM message m
                        JOIN message_event me ON me.message_id = m.id AND me.type = 'EMITTED'
                        WHERE m.topic = :topic
                        """
                    )
                    .namedParam("topic", stubTopic)
                    .firstResult {
                        it.getObject("message_id", UUID::class.java) to
                            (it.getArray("cooperation_lineage").array as Array<*>)
                                .map { it as java.util.UUID }
                                .toTypedArray()
                    }
                    .get()

            val (messageId, parentLineage) = stubMessageData
            val childLineage = parentLineage + java.util.UUID.randomUUID()

            for (type in listOf("SEEN", "COMMITTED")) {
                fluentJdbc
                    .query()
                    .update(
                        """
                        INSERT INTO message_event (message_id, type, cooperation_lineage, coroutine_name, context)
                        VALUES (:message_id, '${type}'::message_event_type, :lineage, :handler, '{}'::jsonb)
                        """
                    )
                    .namedParam("message_id", messageId)
                    .namedParam("lineage", childLineage)
                    .namedParam("handler", stubHandlerName)
                    .run()
            }
        }
    }

    @Test
    fun `repeating step resumes after externally writing SEEN and COMMITTED for stub handler`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)
        val firstIterLatch = CountDownLatch(1)
        val secondIterLatch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", strategyWithStubHandler()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, iteration ->
                            val count = loopCounter.incrementAndGet()
                            executionOrder.add("loop-iter-$iteration")

                            if (count == 1) {
                                scope.launch(
                                    stubTopic,
                                    jsonbHelper.toPGobject(mapOf("task" to "wait-for-human")),
                                )
                                firstIterLatch.countDown()
                            } else if (count == 2) {
                                secondIterLatch.countDown()
                            }
                            NextStep.Repeat
                        },
                    )
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("initial" to "true")),
                )
            }

            Assertions.assertTrue(
                firstIterLatch.await(10, TimeUnit.SECONDS),
                "First iteration did not complete",
            )
            ciSleep(500)
            Assertions.assertEquals(1, loopCounter.get(), "Should be blocked after first iteration")

            externallyCompleteStubHandler()

            Assertions.assertTrue(
                secondIterLatch.await(10, TimeUnit.SECONDS),
                "Second iteration did not complete after externally writing SEEN+COMMITTED. " +
                    "Events: ${executionOrder}",
            )
            Assertions.assertTrue(
                loopCounter.get() >= 2,
                "Parent should have executed at least twice after stub handler completed",
            )
        } finally {
            rootSubscription.close()
        }
    }
}

package io.github.gabrielshanahan.scoop.reactive.coroutine.builder

import io.github.gabrielshanahan.scoop.reactive.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.reactive.messaging.eventLoopStrategy
import io.quarkus.test.junit.QuarkusTest
import io.vertx.core.json.JsonObject
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TryFinallyTest : StructuredCooperationTest() {

    @Test
    fun `finally is executed on success`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val finallyTopic = "finally-topic"
        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    tryFinallyStep(
                        invoke = { scope, message ->
                            executionOrder.add("root-try")
                            val childPayload = JsonObject().put("from", "root-handler")

                            scope.launch(childTopic, childPayload).await().indefinitely()
                        },
                        finally = { scope, message ->
                            executionOrder.add("root-finally")
                            val finallyPayload = JsonObject().put("from", "root-handler")

                            scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("root-end")
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> executionOrder.add("child-handler") }
                },
            )

        val finallySubscription =
            messageQueue.subscribe(
                finallyTopic,
                saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> executionOrder.add("finally-handler") }
                },
            )

        try {
            val rootPayload = JsonObject().put("initial", "true")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertEquals(
                listOf("root-try", "child-handler", "root-finally", "finally-handler", "root-end"),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
            finallySubscription.close()
        }
    }

    @Test
    fun `finally is executed on root failure but messages are not emitted, because neither were those in the 'try' step`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val finallyTopic = "finally-topic"
        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    tryFinallyStep(
                        invoke = { scope, message ->
                            executionOrder.add("root-try")
                            throw RuntimeException("Simulated failure to test rollback")
                        },
                        finally = { scope, message ->
                            executionOrder.add("root-finally")
                            val finallyPayload = JsonObject().put("from", "root-handler")

                            scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                            latch.countDown()
                        },
                    )
                },
            )

        val finallySubscription =
            messageQueue.subscribe(
                finallyTopic,
                saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> executionOrder.add("finally-handler") }
                },
            )

        try {
            val rootPayload = JsonObject().put("initial", "true")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertEquals(
                listOf("root-try", "root-finally"),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            rootSubscription.close()
            finallySubscription.close()
        }
    }

    @Test
    fun `finally is executed on child failure`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val finallyTopic = "finally-topic"
        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    tryFinallyStep(
                        invoke = { scope, message ->
                            executionOrder.add("root-try")
                            val childPayload = JsonObject().put("from", "root-handler")

                            scope.launch(childTopic, childPayload).await().indefinitely()
                        },
                        finally = { scope, message ->
                            executionOrder.add("root-finally")
                            val finallyPayload = JsonObject().put("from", "root-handler")

                            scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                        },
                    )
                    step { scope, message -> executionOrder.add("root-end") }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("child-handler")
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                },
            )

        val finallySubscription =
            messageQueue.subscribe(
                finallyTopic,
                saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("finally-handler")
                        latch.countDown()
                    }
                },
            )

        try {
            val rootPayload = JsonObject().put("initial", "true")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertEquals(
                listOf("root-try", "child-handler", "root-finally", "finally-handler"),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
            finallySubscription.close()
        }
    }

    @Test
    fun `finally is executed, once, on subsequent step failure`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val finallyTopic = "finally-topic"
        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        invoke = { scope, message -> executionOrder.add("root-start") },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("root-rollback")
                            latch.countDown()
                        },
                    )
                    tryFinallyStep(
                        invoke = { scope, message -> executionOrder.add("root-try") },
                        finally = { scope, message ->
                            executionOrder.add("root-finally")
                            val finallyPayload = JsonObject().put("from", "root-handler")

                            scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("root-failure")
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                },
            )

        val finallySubscription =
            messageQueue.subscribe(
                finallyTopic,
                saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message -> executionOrder.add("finally-handler") }
                },
            )

        try {
            val rootPayload = JsonObject().put("initial", "true")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertEquals(
                listOf(
                    "root-start",
                    "root-try",
                    "root-finally",
                    "finally-handler",
                    "root-failure",
                    "root-rollback",
                ),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            rootSubscription.close()
            finallySubscription.close()
        }
    }

    @Test
    fun `finally is only executed once when its child causes a rollback`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val finallyTopic = "finally-topic"
        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        invoke = { scope, message -> executionOrder.add("root-start") },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("root-rollback")
                            latch.countDown()
                        },
                    )
                    tryFinallyStep(
                        invoke = { scope, message -> executionOrder.add("root-try") },
                        finally = { scope, message ->
                            executionOrder.add("root-finally")
                            val finallyPayload = JsonObject().put("from", "root-handler")

                            scope.launch(finallyTopic, finallyPayload).await().indefinitely()
                        },
                    )
                },
            )

        val finallySubscription =
            messageQueue.subscribe(
                finallyTopic,
                saga("finally-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("finally-handler")
                        throw RuntimeException("Simulated failure to test rollback")
                    }
                },
            )

        try {
            val rootPayload = JsonObject().put("initial", "true")
            pool
                .withTransaction { connection ->
                    messageQueue.launch(connection, rootTopic, rootPayload)
                }
                .await()
                .indefinitely()

            Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS), "All handlers should complete")
            Thread.sleep(200)

            Assertions.assertEquals(
                listOf(
                    "root-start",
                    "root-try",
                    "root-finally",
                    "finally-handler",
                    "root-rollback",
                ),
                executionOrder,
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            rootSubscription.close()
            finallySubscription.close()
        }
    }
}

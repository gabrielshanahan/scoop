package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import com.fasterxml.jackson.annotation.JsonTypeName
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.Handler
import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.Topic
import io.github.gabrielshanahan.scoop.coroutine.VariableName
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.util.PGobject

@JsonTypeName("TestResult") object TestResult : VariableName()

@JsonTypeName("AnotherResult") object AnotherResult : VariableName()

// Test topics
private object ChildTestTopic : Topic<Any>()

private object ChildTestTopic2 : Topic<Any>()

// Test handlers - used as type-safe keys for return value retrieval.
// Their handlerName (class simple name) must match the saga name used in subscribe().
object ChildHandler : Handler<Any>(ChildTestTopic) {
    override fun implementation(): DistributedCoroutine =
        error("Test handler - implementation provided inline")
}

object ChildHandler1 : Handler<Any>(ChildTestTopic) {
    override fun implementation(): DistributedCoroutine =
        error("Test handler - implementation provided inline")
}

object ChildHandler2 : Handler<Any>(ChildTestTopic2) {
    override fun implementation(): DistributedCoroutine =
        error("Test handler - implementation provided inline")
}

/** A handler that doesn't exist in any test - used to verify null return for missing handlers. */
private object NonexistentHandler : Handler<Any>(object : Topic<Any>() {}) {
    override fun implementation(): DistributedCoroutine = error("Test handler - not used")
}

/** Maps handler name strings (from DB) to Handler objects for test assertions. */
private val testHandlerRegistry: (String) -> Handler<*> = { name ->
    when (name) {
        "ChildHandler" -> ChildHandler
        "ChildHandler1" -> ChildHandler1
        "ChildHandler2" -> ChildHandler2
        else -> error("Unknown handler: $name")
    }
}

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReturnValueTest : StructuredCooperationTest() {

    @Test
    fun `child handler can store a return value that parent retrieves`() {
        val retrievedValues = AtomicReference<Map<Handler<*>, PGobject>>()
        val latch = CountDownLatch(2)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.launch(childTopic, jsonbHelper.toPGobject(mapOf("task" to "compute")))
                        latch.countDown()
                    }
                    step { scope, _ ->
                        retrievedValues.set(scope.getReturnValues(TestResult, testHandlerRegistry))
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("ChildHandler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.storeReturnValue(
                            TestResult,
                            jsonbHelper.toPGobject(mapOf("answer" to 42)),
                        )
                    }
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("start" to true)),
                )
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "All handlers should complete")
            ciSleep(100)

            val values = retrievedValues.get()
            assertNotNull(values, "Return values should be retrieved")
            assertEquals(1, values.size, "Should have one return value from ChildHandler")
            assertTrue(values.containsKey(ChildHandler), "Key should be the ChildHandler object")

            val returnedJson = values[ChildHandler]!!.value!!
            assertTrue(returnedJson.contains("42"), "Return value should contain the stored data")
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Test
    fun `multiple child handlers can each store return values`() {
        val retrievedValues = AtomicReference<Map<Handler<*>, PGobject>>()
        val latch = CountDownLatch(2)

        val childTopic2 = "child-topic-2"

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.launch(childTopic, jsonbHelper.toPGobject(mapOf("task" to "a")))
                        scope.launch(childTopic2, jsonbHelper.toPGobject(mapOf("task" to "b")))
                        latch.countDown()
                    }
                    step { scope, _ ->
                        retrievedValues.set(scope.getReturnValues(TestResult, testHandlerRegistry))
                        latch.countDown()
                    }
                },
            )

        val childSubscription1 =
            messageQueue.subscribe(
                childTopic,
                saga("ChildHandler1", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.storeReturnValue(
                            TestResult,
                            jsonbHelper.toPGobject(mapOf("result" to "from-child-1")),
                        )
                    }
                },
            )

        val childSubscription2 =
            messageQueue.subscribe(
                childTopic2,
                saga("ChildHandler2", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.storeReturnValue(
                            TestResult,
                            jsonbHelper.toPGobject(mapOf("result" to "from-child-2")),
                        )
                    }
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("start" to true)),
                )
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "All handlers should complete")
            ciSleep(100)

            val values = retrievedValues.get()
            assertNotNull(values, "Return values should be retrieved")
            assertEquals(2, values.size, "Should have return values from both children")
            assertTrue(values.containsKey(ChildHandler1))
            assertTrue(values.containsKey(ChildHandler2))

            assertTrue(values[ChildHandler1]!!.value!!.contains("from-child-1"))
            assertTrue(values[ChildHandler2]!!.value!!.contains("from-child-2"))
        } finally {
            rootSubscription.close()
            childSubscription1.close()
            childSubscription2.close()
        }
    }

    @Test
    fun `getReturnValue retrieves a specific child's return value by handler`() {
        val specificValue = AtomicReference<PGobject?>()
        val missingValue = AtomicReference<PGobject?>(PGobject()) // sentinel to detect null
        val latch = CountDownLatch(2)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.launch(childTopic, jsonbHelper.toPGobject(mapOf("task" to "compute")))
                        latch.countDown()
                    }
                    step { scope, _ ->
                        specificValue.set(scope.getReturnValue(TestResult, ChildHandler))
                        missingValue.set(scope.getReturnValue(TestResult, NonexistentHandler))
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("ChildHandler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.storeReturnValue(
                            TestResult,
                            jsonbHelper.toPGobject(mapOf("value" to "found")),
                        )
                    }
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("start" to true)),
                )
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "All handlers should complete")
            ciSleep(100)

            assertNotNull(specificValue.get(), "Should find ChildHandler's return value")
            assertTrue(specificValue.get()!!.value!!.contains("found"))
            assertNull(missingValue.get(), "Should return null for nonexistent handler")
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Test
    fun `different variable names are independent`() {
        val testResults = AtomicReference<Map<Handler<*>, PGobject>>()
        val anotherResults = AtomicReference<Map<Handler<*>, PGobject>>()
        val latch = CountDownLatch(2)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.launch(childTopic, jsonbHelper.toPGobject(mapOf("task" to "multi")))
                        latch.countDown()
                    }
                    step { scope, _ ->
                        testResults.set(scope.getReturnValues(TestResult, testHandlerRegistry))
                        anotherResults.set(
                            scope.getReturnValues(AnotherResult, testHandlerRegistry)
                        )
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("ChildHandler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, _ ->
                        scope.storeReturnValue(
                            TestResult,
                            jsonbHelper.toPGobject(mapOf("type" to "test")),
                        )
                        scope.storeReturnValue(
                            AnotherResult,
                            jsonbHelper.toPGobject(mapOf("type" to "another")),
                        )
                    }
                },
            )

        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    rootTopic,
                    jsonbHelper.toPGobject(mapOf("start" to true)),
                )
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "All handlers should complete")
            ciSleep(100)

            val test = testResults.get()
            val another = anotherResults.get()

            assertEquals(1, test.size)
            assertEquals(1, another.size)
            assertTrue(test[ChildHandler]!!.value!!.contains("test"))
            assertTrue(another[ChildHandler]!!.value!!.contains("another"))
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }
}

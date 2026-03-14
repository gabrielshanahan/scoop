package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.NextStep
import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LoopTest : StructuredCooperationTest() {

    @Test
    fun `step returning Repeat re-executes with incremented iteration`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(4) // 3 loop iterations + 1 next step

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, iteration ->
                            executionOrder.add("loop-step-iter-$iteration")
                            latch.countDown()
                            if (loopCounter.incrementAndGet() < 3) {
                                NextStep.Repeat
                            } else {
                                NextStep.Continue
                            }
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("after-loop-step")
                        latch.countDown()
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(100)

            Assertions.assertEquals(
                listOf(
                    "loop-step-iter-0",
                    "loop-step-iter-1",
                    "loop-step-iter-2",
                    "after-loop-step",
                ),
                executionOrder,
                "Loop step should execute 3 times then advance",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Test
    fun `zero-iteration loop behaves like normal step`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(2)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "immediate-continue",
                        invoke = { scope, message, iteration ->
                            executionOrder.add("step-1-iter-$iteration")
                            latch.countDown()
                            NextStep.Continue
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("step-2")
                        latch.countDown()
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(100)

            Assertions.assertEquals(
                listOf("step-1-iter-0", "step-2"),
                executionOrder,
                "Immediate Continue behaves like a normal step",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `loop with child launches waits for each batch`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(5) // 2 loop iterations + 2 child steps + 1 next step

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, iteration ->
                            val childPayload =
                                jsonbHelper.toPGobject(mapOf("from" to "loop-iter-$iteration"))
                            scope.launch(childTopic, childPayload)
                            executionOrder.add("loop-step-iter-$iteration")
                            latch.countDown()
                            if (loopCounter.incrementAndGet() < 2) {
                                NextStep.Repeat
                            } else {
                                NextStep.Continue
                            }
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("after-loop-step")
                        latch.countDown()
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        ciSleep(50)
                        executionOrder.add("child-handler-step")
                        latch.countDown()
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(100)

            // Each loop iteration should wait for its children before the next iteration
            Assertions.assertEquals(
                listOf(
                    "loop-step-iter-0",
                    "child-handler-step",
                    "loop-step-iter-1",
                    "child-handler-step",
                    "after-loop-step",
                ),
                executionOrder,
                "Each loop iteration waits for child handlers before next iteration",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `handleChildFailures receives correct childFailureHandlerIteration`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(1) // root rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-with-children",
                        invoke = { scope, message, stepIteration ->
                            scope.launch(
                                childTopic,
                                jsonbHelper.toPGobject(mapOf("from" to "root")),
                            )
                            executionOrder.add("invoke-iter-$stepIteration")
                            NextStep.Continue
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback")
                            latch.countDown()
                        },
                        handleChildFailures = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep ->
                            executionOrder.add(
                                "handleChildFailures-childFailureHandlerIteration-$childFailureHandlerIteration"
                            )
                            if (childFailureHandlerIteration < 1) {
                                // First failure handling: retry by launching another child
                                scope.launch(
                                    childTopic,
                                    jsonbHelper.toPGobject(mapOf("from" to "retry")),
                                )
                                nextStep
                            } else {
                                // Second failure handling: give up
                                throw throwable
                            }
                        },
                    )
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("child-step")
                        @Suppress("TooGenericExceptionThrown")
                        throw RuntimeException("Simulated child failure")
                    }
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(200)

            Assertions.assertEquals(
                listOf(
                    "invoke-iter-0",
                    "child-step",
                    "handleChildFailures-childFailureHandlerIteration-0",
                    "child-step",
                    "handleChildFailures-childFailureHandlerIteration-1",
                    "rollback",
                ),
                executionOrder,
                "handleChildFailures should receive incrementing childFailureHandlerIteration values",
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `mid-loop failure triggers rollback for each iteration in reverse order`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(1) // wait for final rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-iter-$stepIteration")
                            if (loopCounter.incrementAndGet() < 3) {
                                NextStep.Repeat
                            } else {
                                // Fail on iteration 2
                                @Suppress("TooGenericExceptionThrown")
                                throw RuntimeException("Fail on iteration 2")
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-iter-$stepIteration")
                            if (stepIteration == 0) {
                                latch.countDown()
                            }
                        },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(200)

            // Iteration 2 threw during invoke, so its transaction rolled back and no
            // persistent state was committed. Rollback only covers committed iterations 0 and 1.
            // stepIteration in rollback is always 0 (hardcoded).
            Assertions.assertEquals(
                listOf(
                    "invoke-iter-0",
                    "invoke-iter-1",
                    "invoke-iter-2",
                    "rollback-iter-0",
                    "rollback-iter-0",
                ),
                executionOrder,
                "Rollback should traverse committed iterations in reverse order",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Test
    fun `single iteration loop with rollback behaves like normal step rollback`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-loop-$stepIteration")
                            NextStep.Continue
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-loop")
                            latch.countDown()
                        },
                    )
                    step(
                        "failing-step",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-failing")
                            @Suppress("TooGenericExceptionThrown") throw RuntimeException("Failure")
                        },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(200)

            Assertions.assertEquals(
                listOf("invoke-loop-0", "invoke-failing", "rollback-loop"),
                executionOrder,
                "Single-iteration loop rollback behaves like normal step rollback",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `completed loop followed by later failure rolls back all iterations`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(1) // wait for last rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-loop-$stepIteration")
                            if (loopCounter.incrementAndGet() < 3) {
                                NextStep.Repeat
                            } else {
                                NextStep.Continue
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-loop")
                            if (executionOrder.count { it == "rollback-loop" } >= 3) {
                                latch.countDown()
                            }
                        },
                    )
                    step(
                        "middle-step",
                        invoke = { scope, message -> executionOrder.add("invoke-middle") },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("rollback-middle")
                        },
                    )
                    step(
                        "failing-step",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-failing")
                            @Suppress("TooGenericExceptionThrown")
                            throw RuntimeException("Step after loop fails")
                        },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(200)

            // Flow: loop(0)→loop(1)→loop(2,Continue)→middle→failing(throws)
            // Rollback covers committed SUSPENDED steps in reverse chronological order:
            //   middle, loop(iter-2), loop(iter-1), loop(iter-0)
            // failing-step threw during invoke, so no SUSPENDED was committed for it.
            Assertions.assertEquals(
                listOf(
                    "invoke-loop-0",
                    "invoke-loop-1",
                    "invoke-loop-2",
                    "invoke-middle",
                    "invoke-failing",
                    "rollback-middle",
                    "rollback-loop",
                    "rollback-loop",
                    "rollback-loop",
                ),
                executionOrder,
                "Rollback should cover middle-step and all 3 loop iterations in reverse",
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `multiple loop steps followed by failure roll back all iterations of both`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopACounter = AtomicInteger(0)
        val loopBCounter = AtomicInteger(0)

        val latch = CountDownLatch(1) // wait for last rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-A-$stepIteration")
                            if (loopACounter.incrementAndGet() < 2) {
                                NextStep.Repeat
                            } else {
                                NextStep.Continue
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-A")
                            if (executionOrder.count { it == "rollback-A" } >= 2) {
                                latch.countDown()
                            }
                        },
                    )
                    step(
                        "loop-B",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-B-$stepIteration")
                            if (loopBCounter.incrementAndGet() < 2) {
                                NextStep.Repeat
                            } else {
                                NextStep.Continue
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-B")
                        },
                    )
                    step(
                        "failing-step",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-failing")
                            @Suppress("TooGenericExceptionThrown")
                            throw RuntimeException("Final step fails")
                        },
                    )
                },
            )

        try {
            val rootPayload = jsonbHelper.toPGobject(mapOf("initial" to "true"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, rootPayload)
            }

            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Not everything completed correctly",
            )
            ciSleep(200)

            // Flow: A(0)→A(1,Continue)→B(0)→B(1,Continue)→failing(throws)
            // Rollback in reverse chronological: B(1), B(0), A(1), A(0)
            Assertions.assertEquals(
                listOf(
                    "invoke-A-0",
                    "invoke-A-1",
                    "invoke-B-0",
                    "invoke-B-1",
                    "invoke-failing",
                    "rollback-B",
                    "rollback-B",
                    "rollback-A",
                    "rollback-A",
                ),
                executionOrder,
                "Rollback should cover all iterations of both loop steps in reverse order",
            )
        } finally {
            rootSubscription.close()
        }
    }
}

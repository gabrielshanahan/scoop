package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.NextStep
import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.ciSleep
import io.github.gabrielshanahan.scoop.coroutine.getEventSequence
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
class GoToTest : StructuredCooperationTest() {

    @Test
    fun `GoTo forward skips intermediate steps`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(2) // step-0 + step-2

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-0",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("step-0")
                            latch.countDown()
                            NextStep.GoTo(2)
                        },
                    )
                    step("step-1") { scope, message ->
                        executionOrder.add("step-1-SHOULD-NOT-EXECUTE")
                        latch.countDown()
                    }
                    step("step-2") { scope, message ->
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
                listOf("step-0", "step-2"),
                executionOrder,
                "GoTo(2) should skip step-1 entirely",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-0", "root-handler"),
                    Triple("SUSPENDED", "step-2", "root-handler"),
                    Triple("COMMITTED", "step-2", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo backward re-executes from target step`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val cycleCount = AtomicInteger(0)

        // A→B→C→GoTo(0)→A→B→C = 6 step executions
        val latch = CountDownLatch(6)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("A-crc-$stepIteration")
                            latch.countDown()
                            NextStep.Continue
                        },
                    )
                    step("step-B") { scope, message ->
                        executionOrder.add("B")
                        latch.countDown()
                    }
                    step(
                        "step-C",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("C")
                            latch.countDown()
                            if (cycleCount.incrementAndGet() < 2) {
                                NextStep.GoTo(0)
                            } else {
                                NextStep.Continue
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
            ciSleep(100)

            // GoTo backward works: step A re-executes after GoTo(0) from step C.
            // stepIteration for second A is 0 because B and C ran in between,
            // breaking the consecutive chain.
            Assertions.assertEquals(
                listOf("A-crc-0", "B", "C", "A-crc-0", "B", "C"),
                executionOrder,
                "GoTo(0) should re-execute from step A with crc reset to 0",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-B", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-B", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("COMMITTED", "step-C", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo self behaves like Repeat`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(4) // 3 self-goto iterations + 1 next step

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "self-goto-step",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("self-goto-iter-$stepIteration")
                            latch.countDown()
                            if (loopCounter.incrementAndGet() < 3) {
                                NextStep.GoTo(0) // GoTo own index
                            } else {
                                NextStep.Continue
                            }
                        },
                    )
                    step { scope, message ->
                        executionOrder.add("after-self-goto")
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
                    "self-goto-iter-0",
                    "self-goto-iter-1",
                    "self-goto-iter-2",
                    "after-self-goto",
                ),
                executionOrder,
                "GoTo(ownIndex) should behave identically to Repeat",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "self-goto-step", "root-handler"),
                    Triple("SUSPENDED", "self-goto-step", "root-handler"),
                    Triple("SUSPENDED", "self-goto-step", "root-handler"),
                    Triple("SUSPENDED", "1", "root-handler"),
                    Triple("COMMITTED", "1", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo plus rollback rolls back in reverse chronological order`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val stepACount = AtomicInteger(0)

        val latch = CountDownLatch(1) // wait for last rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-A")
                            if (stepACount.incrementAndGet() < 2) {
                                NextStep.Continue
                            } else {
                                // Second time: skip B, go directly to C
                                NextStep.GoTo(2)
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
                        "step-B",
                        invoke = { scope, message -> executionOrder.add("invoke-B") },
                        rollback = { scope, message, throwable -> executionOrder.add("rollback-B") },
                    )
                    step(
                        "step-C",
                        invoke = { scope, message, stepIteration ->
                            val cCount = executionOrder.count { it == "invoke-C" }
                            executionOrder.add("invoke-C")
                            if (cCount > 0) {
                                // Second time C runs: fail
                                @Suppress("TooGenericExceptionThrown")
                                throw RuntimeException("Fail on second C")
                            }
                            NextStep.GoTo(0)
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-C")
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

            // Flow: A→B→C(GoTo(0))→A(GoTo(2))→C(fail)
            // Rollback covers committed SUSPENDED steps in reverse chronological order:
            //   A(2nd), C(1st), B, A(1st)
            // stepIteration in rollback is always 0 (hardcoded in RollbackPathContinuation)
            Assertions.assertEquals(
                listOf(
                    "invoke-A",
                    "invoke-B",
                    "invoke-C",
                    "invoke-A",
                    "invoke-C",
                    "rollback-A",
                    "rollback-C",
                    "rollback-B",
                    "rollback-A",
                ),
                executionOrder,
                "Rollback should traverse committed steps in reverse chronological order",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-B", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("ROLLING_BACK", "step-C", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-A[1,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-A[1,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-C[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-B[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-B[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-A[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `loop with children rolls back each iteration's children in reverse`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val loopCounter = AtomicInteger(0)

        val latch = CountDownLatch(1) // wait for root rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "loop-step",
                        invoke = { scope, message, stepIteration ->
                            scope.launch(
                                childTopic,
                                jsonbHelper.toPGobject(mapOf("iter" to "$stepIteration")),
                            )
                            executionOrder.add("invoke-loop-$stepIteration")
                            if (loopCounter.incrementAndGet() < 2) {
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
                            latch.countDown()
                        },
                    )
                    step(
                        "failing-step",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-failing")
                            @Suppress("TooGenericExceptionThrown")
                            throw RuntimeException("Step after loop fails")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("rollback-failing")
                        },
                    )
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        ciSleep(50)
                        executionOrder.add("child-step")
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

            // Flow: loop-0(child)→loop-1(child)→failing(fail)
            // Rollback: failing-step has no committed SUSPENDED (it failed),
            // then loop-step iteration 1, then loop-step iteration 0
            // Each loop iteration's child rollback emits rollback for that iteration's children
            Assertions.assertEquals(
                listOf(
                    "invoke-loop-0",
                    "child-step",
                    "invoke-loop-1",
                    "child-step",
                    "invoke-failing",
                    "rollback-loop",
                    "rollback-loop",
                ),
                executionOrder,
                "Rollback should cover each loop iteration in reverse order",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("EMITTED", "loop-step", "root-handler"),
                    Triple("SUSPENDED", "loop-step", "root-handler"),
                    Triple("SEEN", null, "child-handler"),
                    Triple("SUSPENDED", "0", "child-handler"),
                    Triple("COMMITTED", "0", "child-handler"),
                    Triple("EMITTED", "loop-step", "root-handler"),
                    Triple("SUSPENDED", "loop-step", "root-handler"),
                    Triple("SEEN", null, "child-handler"),
                    Triple("SUSPENDED", "0", "child-handler"),
                    Triple("COMMITTED", "0", "child-handler"),
                    Triple("ROLLING_BACK", "failing-step", "root-handler"),
                    Triple(
                        "ROLLBACK_EMITTED",
                        "Rollback of loop-step[1,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "SUSPENDED",
                        "Rollback of loop-step[1,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("ROLLING_BACK", null, "child-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of 0[0,] (rolling back child scopes)",
                        "child-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    Triple("SUSPENDED", "Rollback of loop-step[1,]", "root-handler"),
                    Triple(
                        "ROLLBACK_EMITTED",
                        "Rollback of loop-step[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "SUSPENDED",
                        "Rollback of loop-step[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("ROLLING_BACK", null, "child-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of 0[0,] (rolling back child scopes)",
                        "child-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    Triple("SUSPENDED", "Rollback of loop-step[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of loop-step[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `handleChildFailures can override NextStep and childFailureHandlerIteration increments correctly`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(1) // wait for rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-0",
                        invoke = { scope, message, stepIteration ->
                            scope.launch(
                                childTopic,
                                jsonbHelper.toPGobject(mapOf("from" to "step-0")),
                            )
                            executionOrder.add("invoke-0")
                            // invoke returns Continue, but hcf will override to GoTo(0)
                            NextStep.Continue
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-0")
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
                                "hcf-childFailureHandlerIteration-$childFailureHandlerIteration-invoke-$nextStep"
                            )
                            if (childFailureHandlerIteration < 1) {
                                // Override: retry with a new child, change NextStep to GoTo(0)
                                scope.launch(
                                    childTopic,
                                    jsonbHelper.toPGobject(mapOf("from" to "hcf-retry")),
                                )
                                NextStep.GoTo(0) // Override Continue → GoTo(0)
                            } else {
                                // Give up on second failure
                                throw throwable
                            }
                        },
                    )
                    step("step-1") { scope, message ->
                        executionOrder.add("invoke-1-SHOULD-NOT-EXECUTE")
                    }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("child-step")
                        @Suppress("TooGenericExceptionThrown")
                        throw RuntimeException("Child always fails")
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

            // Flow: invoke-0 (Continue) → child fails → hcf childFailureHandlerIteration=0
            // overrides to GoTo(0),
            // launches retry child → retry child fails → hcf childFailureHandlerIteration=1
            // rethrows → rollback
            // Note: hcf's GoTo(0) becomes the next SUSPENDED's next_step=0, which is
            // reconstructed as Repeat (since step index is also 0)
            Assertions.assertEquals(
                listOf(
                    "invoke-0",
                    "child-step",
                    "hcf-childFailureHandlerIteration-0-invoke-Continue",
                    "child-step",
                    "hcf-childFailureHandlerIteration-1-invoke-Repeat", // Reconstructed from
                    // next_step=0 on step 0
                    "rollback-0",
                ),
                executionOrder,
                "hcf should receive incrementing childFailureHandlerIteration and be able to override NextStep",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("EMITTED", "step-0", "root-handler"),
                    Triple("SUSPENDED", "step-0", "root-handler"),
                    Triple("SEEN", null, "child-handler"),
                    Triple("ROLLING_BACK", "0", "child-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    Triple("EMITTED", "step-0", "root-handler"),
                    Triple("SUSPENDED", "step-0", "root-handler"),
                    Triple("SEEN", null, "child-handler"),
                    Triple("ROLLING_BACK", "0", "child-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    Triple("ROLLING_BACK", "step-0", "root-handler"),
                    Triple(
                        "ROLLBACK_EMITTED",
                        "Rollback of step-0[0,0] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-0[0,0] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "ROLLBACK_EMITTED",
                        "Rollback of step-0[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-0[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-0[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-0[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `handleChildFailures receives correct nextStep for GoTo`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(1) // wait for rollback

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-0",
                        invoke = { scope, message, stepIteration ->
                            scope.launch(
                                childTopic,
                                jsonbHelper.toPGobject(mapOf("from" to "step-0")),
                            )
                            executionOrder.add("invoke-0")
                            NextStep.GoTo(2) // invoke returns GoTo(2)
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-0")
                            latch.countDown()
                        },
                        handleChildFailures = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep ->
                            // Verify nextStep is GoTo(2) from the original invoke
                            executionOrder.add(
                                "hcf-childFailureHandlerIteration-$childFailureHandlerIteration-nextStep-$nextStep"
                            )
                            // The only way to exit hcf is to rethrow
                            throw throwable
                        },
                    )
                    step("step-1") { scope, message -> executionOrder.add("invoke-1") }
                    step("step-2") { scope, message -> executionOrder.add("invoke-2") }
                },
            )

        val childSubscription =
            messageQueue.subscribe(
                childTopic,
                saga("child-handler", handlerRegistry.eventLoopStrategy()) {
                    step { scope, message ->
                        executionOrder.add("child-failing")
                        @Suppress("TooGenericExceptionThrown")
                        throw RuntimeException("Child always fails")
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

            // invoke returns GoTo(2), child fails, hcf receives nextStep=GoTo(2)
            // and rethrows → rollback. The nextStep proves GoTo is correctly
            // reconstructed from the SUSPENDED event's next_step column.
            Assertions.assertEquals(
                listOf(
                    "invoke-0",
                    "child-failing",
                    "hcf-childFailureHandlerIteration-0-nextStep-GoTo(stepIndex=2)",
                    "rollback-0",
                ),
                executionOrder,
                "hcf should receive the original GoTo nextStep",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("EMITTED", "step-0", "root-handler"),
                    Triple("SUSPENDED", "step-0", "root-handler"),
                    Triple("SEEN", null, "child-handler"),
                    Triple("ROLLING_BACK", "0", "child-handler"),
                    Triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    Triple("ROLLING_BACK", "step-0", "root-handler"),
                    Triple(
                        "ROLLBACK_EMITTED",
                        "Rollback of step-0[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-0[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-0[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-0[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
            childSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo forward skip does not include skipped step in rollback`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())

        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-A")
                            NextStep.GoTo(2)
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-A")
                            latch.countDown()
                        },
                    )
                    step(
                        "step-B",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-B-SHOULD-NOT-EXECUTE")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("rollback-B-SHOULD-NOT-EXECUTE")
                        },
                    )
                    step(
                        "step-C",
                        invoke = { scope, message -> executionOrder.add("invoke-C") },
                        rollback = { scope, message, throwable -> executionOrder.add("rollback-C") },
                    )
                    step(
                        "step-D",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-D")
                            @Suppress("TooGenericExceptionThrown")
                            throw RuntimeException("Step D fails")
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

            // Flow: A(GoTo 2)→C→D(fail)
            // Rollback: C, A (step-B was never executed, so not in rollback)
            Assertions.assertEquals(
                listOf("invoke-A", "invoke-C", "invoke-D", "rollback-C", "rollback-A"),
                executionOrder,
                "Skipped step-B should not appear in execution or rollback",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("ROLLING_BACK", "step-D", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-C[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-A[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo forward then GoTo backward with failure rolls back all visited instances`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val stepCCount = AtomicInteger(0)

        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-A")
                            NextStep.GoTo(2) // Skip B, go to C
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-A")
                            latch.countDown()
                        },
                    )
                    step(
                        "step-B",
                        invoke = { scope, message -> executionOrder.add("invoke-B") },
                        rollback = { scope, message, throwable -> executionOrder.add("rollback-B") },
                    )
                    step(
                        "step-C",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-C")
                            if (stepCCount.incrementAndGet() < 2) {
                                NextStep.GoTo(1) // Go back to B
                            } else {
                                @Suppress("TooGenericExceptionThrown")
                                throw RuntimeException("Step C fails on second visit")
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-C")
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

            // Flow: A(GoTo 2)→C(GoTo 1)→B→C(fail)
            // Committed: A, C(1st), B. C's 2nd invoke threw, not committed.
            // Rollback in reverse chronological: B, C(1st), A
            Assertions.assertEquals(
                listOf(
                    "invoke-A",
                    "invoke-C",
                    "invoke-B",
                    "invoke-C",
                    "rollback-B",
                    "rollback-C",
                    "rollback-A",
                ),
                executionOrder,
                "Rollback covers all visited steps in reverse chronological order",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("SUSPENDED", "step-B", "root-handler"),
                    Triple("ROLLING_BACK", "step-C", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-B[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-B[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-C[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-A[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }

    @Suppress("LongMethod")
    @Test
    fun `GoTo to repeating step then failure rolls back all instances`() {
        val executionOrder = Collections.synchronizedList(mutableListOf<String>())
        val repeatCount = AtomicInteger(0)

        val latch = CountDownLatch(1)

        val rootSubscription =
            messageQueue.subscribe(
                rootTopic,
                saga("root-handler", handlerRegistry.eventLoopStrategy()) {
                    step(
                        "step-A",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-A")
                            NextStep.GoTo(2)
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-A")
                            latch.countDown()
                        },
                    )
                    step(
                        "step-B",
                        invoke = { scope, message ->
                            executionOrder.add("invoke-B-SHOULD-NOT-EXECUTE")
                        },
                        rollback = { scope, message, throwable ->
                            executionOrder.add("rollback-B-SHOULD-NOT-EXECUTE")
                        },
                    )
                    step(
                        "step-C",
                        invoke = { scope, message, stepIteration ->
                            executionOrder.add("invoke-C-$stepIteration")
                            if (repeatCount.incrementAndGet() < 2) {
                                NextStep.Repeat
                            } else {
                                @Suppress("TooGenericExceptionThrown")
                                throw RuntimeException("Step C fails after repeating")
                            }
                        },
                        rollback = {
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration ->
                            executionOrder.add("rollback-C")
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

            // Flow: A(GoTo 2)→C(iter 0, Repeat)→C(iter 1, fail)
            // Committed: A, C(iter 0). C iter 1 threw, not committed.
            // Rollback: C(iter 0), A — step B never executed, not in rollback
            Assertions.assertEquals(
                listOf("invoke-A", "invoke-C-0", "invoke-C-1", "rollback-C", "rollback-A"),
                executionOrder,
                "GoTo to repeating step: rollback covers committed instances, skipped step excluded",
            )

            Assertions.assertEquals(
                listOf(
                    Triple("EMITTED", null, null),
                    Triple("SEEN", null, "root-handler"),
                    Triple("SUSPENDED", "step-A", "root-handler"),
                    Triple("SUSPENDED", "step-C", "root-handler"),
                    Triple("ROLLING_BACK", "step-C", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-C[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                    Triple(
                        "SUSPENDED",
                        "Rollback of step-A[0,] (rolling back child scopes)",
                        "root-handler",
                    ),
                    Triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                    Triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
                ),
                fluentJdbc.getEventSequence(),
            )
        } finally {
            rootSubscription.close()
        }
    }
}

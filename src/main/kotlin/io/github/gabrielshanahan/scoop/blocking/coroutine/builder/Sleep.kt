package io.github.gabrielshanahan.scoop.blocking.coroutine.builder

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.has
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.BaseEventLoopStrategy
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import org.intellij.lang.annotations.Language
import org.postgresql.util.PGobject

/**
 * Scheduling and delay functionality for distributed sagas using time-based cooperation.
 * 
 * This module implements sleep/scheduling functionality using structured cooperation principles.
 * Instead of blocking threads or using timers, sagas emit messages to a special sleep topic
 * and resume when the specified time has elapsed.
 * 
 * ## How Sleep Works
 * 
 * 1. **Sleep request**: Saga emits a message to the sleep topic with a [SleepUntil] context
 * 2. **Sleep handler**: A dedicated handler (automatically registered) receives the message
 * 3. **Time check**: The [SleepEventLoopStrategy] only allows resumption after the wake time
 * 4. **Resume**: When the time arrives, the original saga resumes its next step
 * 
 * ## Features Enabled
 * 
 * - **Basic delays**: `sleepForStep(duration)` pauses saga execution
 * - **Scheduled execution**: `scheduledStep(wakeAfter, logic)` runs logic at a specific time  
 * - **Periodic tasks**: `periodic(runEvery, runCount)` creates recurring operations
 * 
 * ## Implementation Notes
 * 
 * The sleep topic handler is automatically subscribed in PostgresMessageQueue, making this
 * functionality available out-of-the-box without additional setup.
 * 
 * The unique topic UUID prevents accidental collisions with user-defined topics.
 * 
 * For examples and patterns, see the sleep tests and the blog post:
 * https://developer.porn/posts/implementing-structured-cooperation/#deadlines-scheduling
 */

/**
 * Special topic for sleep/scheduling messages. 
 * 
 * This topic is automatically subscribed by PostgresMessageQueue with a dedicated sleep handler.
 * The UUID suffix prevents conflicts with user-defined topics.
 */
const val SLEEP_TOPIC = "sleep-9d24148d-d851-4107-8beb-e5c57f5cca88"

/**
 * Context key for sleep/scheduling information.
 */
data object SleepUntilKey : CooperationContext.MappedKey<SleepUntil>()

/**
 * Context element that specifies when a saga should wake up from sleep.
 * 
 * This element is used by the sleep event loop strategy to determine when
 * a saga should be allowed to resume execution.
 * 
 * @param wakeAfter The absolute time when the saga should resume
 */
data class SleepUntil(val wakeAfter: OffsetDateTime) :
    CooperationContext.MappedElement(SleepUntilKey)

/**
 * Creates a sleep context that wakes up after the specified duration from now.
 * 
 * @param duration How long to sleep
 * @return Sleep context with the calculated wake time
 */
fun sleepFor(duration: Duration): SleepUntil =
    SleepUntil(OffsetDateTime.now().plus(duration.inWholeMicroseconds, ChronoUnit.MICROS))

/**
 * Creates a sleep context that wakes up at the specified absolute time.
 * 
 * @param wakeAfter The absolute time to wake up
 * @return Sleep context with the specified wake time
 */
fun sleepUntil(wakeAfter: OffsetDateTime): SleepUntil = SleepUntil(wakeAfter)

/**
 * Event loop strategy for the sleep handler that only resumes when the wake time has arrived.
 * 
 * This strategy implements time-based resumption by checking the [SleepUntil] context element
 * and comparing it against the current database time. The sleep handler will only resume
 * execution when `CLOCK_TIMESTAMP()` is past the specified wake time.
 * 
 * ## SQL Implementation
 * 
 * The strategy uses PostgreSQL's `CLOCK_TIMESTAMP()` function to get the current time,
 * ensuring that time comparisons are consistent with the database's clock regardless
 * of application server time differences.
 * 
 * ## Rollback Behavior
 * 
 * Sleep handlers never resume on rollback path (returns FALSE), since sleep operations
 * don't typically need compensating actions.
 * 
 * @param ignoreOlderThan Inherited from BaseEventLoopStrategy to ignore very old messages
 */
class SleepEventLoopStrategy(ignoreOlderThan: OffsetDateTime) :
    BaseEventLoopStrategy(ignoreOlderThan) {
    @Language("PostgreSQL")
    override fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String =
        """
        EXISTS (
            SELECT 1
                FROM $candidateSeen
                WHERE jsonb_exists_any_indexed($candidateSeen.context, 'SleepUntilKey')
                    AND ($candidateSeen.context->'SleepUntilKey'->>'wakeAfter')::timestamptz < CLOCK_TIMESTAMP()
        )
    """
            .trimIndent()

    @Language("PostgreSQL")
    override fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String = "FALSE"
}

fun SagaBuilder.sleepForStep(name: String, duration: Duration) {
    step(
        invoke = { scope, _ ->
            scope.launch(
                SLEEP_TOPIC,
                PGobject().apply {
                    type = "jsonb"
                    value = "{}"
                },
                sleepFor(duration),
            )
        },
        name = name,
    )
}

fun SagaBuilder.sleepForStep(duration: Duration) = sleepForStep(steps.size.toString(), duration)

fun SagaBuilder.scheduledStep(
    name: String,
    wakeAfter: OffsetDateTime,
    invoke: (CooperationScope, Message) -> Unit,
    rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
) {
    step(
        invoke = { scope, _ ->
            scope.launch(
                SLEEP_TOPIC,
                PGobject().apply {
                    type = "jsonb"
                    value = "{}"
                },
                sleepUntil(wakeAfter),
            )
        },
        name = "$name (waiting for scheduled time)",
    )
    step(name, invoke, rollback, handleChildFailures)
}

fun SagaBuilder.scheduledStep(
    runAfter: OffsetDateTime,
    invoke: (CooperationScope, Message) -> Unit,
    rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
) = scheduledStep(steps.size.toString(), runAfter, invoke, rollback, handleChildFailures)

data object RunCountKey : CooperationContext.MappedKey<RunCount>()

data class RunCount(val value: Int = 0) : CooperationContext.MappedElement(RunCountKey)

fun SagaBuilder.periodic(name: String, runEvery: Duration, runCount: Int) {
    sleepForStep("$name (sleep)", runEvery)
    step(
        name = "$name (launch next)",
        invoke = { scope, message ->
            if (!scope.context.has(RunCountKey)) {
                scope.context += RunCount(0)
            }

            scope.context += RunCount(scope.context[RunCountKey]!!.value + 1)

            if (scope.context[RunCountKey]!!.value < runCount) {
                scope.launchOnGlobalScope(message.topic, message.payload, scope.context)
            }
        },
        rollback = null,
        handleChildFailures = null,
    )
}

fun SagaBuilder.periodic(runEvery: Duration, runCount: Int) =
    periodic(steps.size.toString(), runEvery, runCount)

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
 * This module implements sleep/scheduling functionality. Sagas emit messages to a special sleep
 * topic, which is "bundled with" Scoop, and which uses a
 * [special EventLoopStrategy][SleepEventLoopStrategy] to resume when the specified time has
 * elapsed.
 *
 * ## How Sleep Works
 * 1. **Sleep request**: Saga emits a message to the sleep topic with a [SleepUntil] context
 * 2. **Sleep handler**: A dedicated handler (automatically registered) receives the message
 * 3. **Time check**: The [SleepEventLoopStrategy] only allows resumption after the wake time
 * 4. **Resume**: When the time arrives, the original saga resumes its next step
 *
 * ## Features Enabled
 *
 * ### Basic Delays
 * `sleepForStep(duration)` creates a saga step that pauses execution for a specified duration. The
 * step simply emits a message to the [SLEEP_TOPIC] with a [SleepUntil] context element:
 * ```kotlin
 * saga("delayed-processing") {
 *     step { scope, message ->
 *         // Process immediately
 *         processMessage(message)
 *     }
 *     sleepForStep(Duration.ofMinutes(30))  // Wait 30 minutes
 *     step { scope, message ->
 *         // This runs 30 minutes later
 *         sendFollowUp(message)
 *     }
 * }
 * ```
 *
 * ### Scheduled Steps
 * `scheduledStep(wakeAfter, logic)` enables "execute at specific time" patterns:
 * ```kotlin
 * saga("survey-sender") {
 *     step { scope, message ->
 *         // User completed purchase
 *         recordPurchase(message)
 *     }
 *     scheduledStep(
 *         wakeAfter = OffsetDateTime.now().plusDays(2),
 *         invoke = { scope, message ->
 *             // This runs exactly 2 days after purchase
 *             sendSurveyEmail(message)
 *         }
 *     )
 * }
 * ```
 *
 * The scheduled step creates two actual saga steps:
 * 1. A sleep step that waits until the specified time
 * 2. The logic step that executes when the time arrives
 *
 * ### Periodic Tasks
 * `periodic(runEvery, runCount)` creates self-restarting sagas for recurring operations:
 * ```kotlin
 * saga("health-checker") {
 *     step { scope, message ->
 *         checkSystemHealth()
 *     }
 *     periodic(runEvery = Duration.ofMinutes(5), runCount = 100)
 * }
 * ```
 *
 * **How periodic works**:
 * 1. Sleep for the specified duration
 * 2. Increment run counter in context
 * 3. If counter < runCount, use `launchOnGlobalScope` to launch the same message again, thereby
 *    starting a new, independent run of the entire saga
 * 4. Continue executing the rest of the current saga (the new one starts at 1., so sleeping for the
 *    specified duration)
 *
 * There is an inherent tradeoff in how periodic tasks can be implemented:
 * * You can emit the "plan next run" message *before* executing the actual logic. This guarantees
 *   that a next run is always planned and will always get executed regardless of the result of the
 *   previous run. However, it may cause overlapping runs if the saga runs for longer than the
 *   periodicity.
 * * You can emit the "plan next run" message *after* executing the actual logic. This guarantees
 *   that there are no overlapping runs, however you need to arrange for the message to be emitted
 *   even when the saga run finishes with a failure `nd handle the arithmetic calculating when the
 *   next run should be scheduled (now + max(periodicity - executionTime, 0)).
 *
 * For simplicity, Scoop implements the former approach, even though it is perfectly possible to
 * implement the latter using [tryFinallyStep] and additional[CooperationContext] elements.
 *
 * ## Implementation Notes
 *
 * The sleep topic handler is automatically subscribed in
 * [PostgresMessageQueue][io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue],
 * making this functionality available out-of-the-box without additional setup. The unique
 * [topic UUID][SLEEP_TOPIC] prevents accidental collisions with user-defined topics.
 */

/**
 * Special topic for sleep/scheduling messages.
 *
 * This topic is automatically subscribed in
 * [PostgresMessageQueue][io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue]
 * with a dedicated sleep handler.
 *
 * The UUID suffix prevents conflicts with user-defined topics.
 */
const val SLEEP_TOPIC = "sleep-9d24148d-d851-4107-8beb-e5c57f5cca88"

/** Context key for sleep/scheduling information. */
data object SleepUntilKey : CooperationContext.MappedKey<SleepUntil>()

/**
 * Context element that specifies when a saga should wake up from sleep.
 *
 * This element is used by [SleepEventLoopStrategy] to determine when a saga should be allowed to
 * resume execution.
 *
 * @param wakeAfter The absolute time when the saga should resume
 */
data class SleepUntil(val wakeAfter: OffsetDateTime) :
    CooperationContext.MappedElement(SleepUntilKey)

/**
 * Helper method for creating [SleepUntil] instances using relative time.
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
 * This strategy implements time-based resumption by checking the [SleepUntil] context element and
 * comparing it against the current database time. The sleep handler will only resume execution when
 * `CLOCK_TIMESTAMP()` is past the specified wake time.
 *
 * ## Rollback Behavior
 *
 * Sleep handlers never emit anything, so it's always possible to resume immediately.
 *
 * @param ignoreOlderThan Inherited from [BaseEventLoopStrategy]
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
    ): String = "TRUE"
}

/**
 * Creates a saga step that pauses execution for the specified duration.
 *
 * This function creates a single saga step that emits a sleep message to the [SLEEP_TOPIC]. The
 * saga will suspend at this step and only resume when the [SleepEventLoopStrategy] determines that
 * the specified time has elapsed.
 *
 * ## Structured Cooperation Role
 *
 * The sleep step follows structured cooperation principles:
 * - The saga suspends after emitting the sleep message
 * - Execution only resumes when the sleep handler completes (i.e., when time elapses)
 * - If this step is rolled back, no compensating action is needed
 *
 * ## Usage Example
 *
 * ```kotlin
 * saga("delayed-notification") {
 *     step { scope, message ->
 *         processInitialRequest(message)
 *     }
 *     sleepForStep("wait-for-cooldown", Duration.ofMinutes(15))
 *     step { scope, message ->
 *         sendDelayedNotification(message)
 *     }
 * }
 * ```
 *
 * @param name The name for this sleep step (used in debugging and traces)
 * @param duration How long to sleep before resuming
 */
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

/**
 * Creates a sleep step with an auto-generated name based on the current step count.
 *
 * This is a convenience overload of [sleepForStep] that automatically generates a step name using
 * the current step index. Useful for quick prototyping or when step naming isn't critical.
 *
 * @param duration How long to sleep before resuming
 * @see sleepForStep for detailed behavior description
 */
fun SagaBuilder.sleepForStep(duration: Duration) = sleepForStep(steps.size.toString(), duration)

/**
 * Creates a scheduled step that executes at a specific absolute time.
 *
 * This function creates two saga steps:
 * 1. A sleep step that waits until the specified time
 * 2. The actual logic step that executes when the time arrives
 *
 * This enables "execute at specific time" patterns like sending follow-up emails or running
 * maintenance tasks at predetermined times.
 *
 * ## Usage Example
 *
 * ```kotlin
 * saga("survey-sender") {
 *     step { scope, message ->
 *         recordPurchase(message)
 *     }
 *     scheduledStep(
 *         name = "send-survey",
 *         wakeAfter = OffsetDateTime.now().plusDays(2),
 *         invoke = { scope, message ->
 *             sendSurveyEmail(extractEmail(message))
 *         },
 *         rollback = { scope, message, throwable ->
 *             logSurveyFailure(message, throwable)
 *         }
 *     )
 * }
 * ```
 *
 * @param name The name for the logic step (sleep step gets "(waiting for scheduled time)" suffix)
 * @param wakeAfter The absolute time when the logic should execute
 * @param invoke The logic to execute at the scheduled time
 * @param rollback Optional compensating action if the logic step fails
 * @param handleChildFailures Optional handler for child saga failures in the logic step
 */
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

/**
 * Creates a scheduled step with an auto-generated name based on the current step count.
 *
 * This is a convenience overload of [scheduledStep] that automatically generates a step name using
 * the current step index.
 *
 * @param runAfter The absolute time when the logic should execute
 * @param invoke The logic to execute at the scheduled time
 * @param rollback Optional compensating action if the logic step fails
 * @param handleChildFailures Optional handler for child saga failures in the logic step
 * @see scheduledStep for detailed behavior description
 */
fun SagaBuilder.scheduledStep(
    runAfter: OffsetDateTime,
    invoke: (CooperationScope, Message) -> Unit,
    rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
) = scheduledStep(steps.size.toString(), runAfter, invoke, rollback, handleChildFailures)

/**
 * Context key for tracking execution count in periodic tasks.
 *
 * Used by [periodic] to track how many times a periodic saga has executed, enabling automatic
 * termination after a specified number of runs.
 */
data object RunCountKey : CooperationContext.MappedKey<RunCount>()

/**
 * Context element that tracks the current execution count for periodic tasks.
 *
 * This element is automatically managed by [periodic] functions:
 * - Initialized to 0 on first run if not present
 * - Incremented before each execution
 * - Used to determine when to stop scheduling new runs
 *
 * @param value The current run count (0-based)
 */
data class RunCount(val value: Int = 0) : CooperationContext.MappedElement(RunCountKey)

/**
 * Creates a periodic task that runs a limited number of times with a specified interval.
 *
 * This function transforms the current saga into a self-restarting periodic task by adding:
 * 1. A sleep step that waits for the specified duration
 * 2. A scheduling step that launches the next run (if the limit hasn't been reached)
 *
 * ## How Periodic Tasks Work
 *
 * The periodic mechanism creates independent saga instances:
 * 1. **Sleep**: Wait for the specified duration
 * 2. **Count management**: Increment the run counter in [CooperationContext]
 * 3. **Next run**: If counter < runCount, use `launchOnGlobalScope` to start a new saga
 * 4. **Independence**: Each run is completely independent (no structured cooperation between runs)
 *
 * ## Execution Ordering Trade-offs
 *
 * Scoop implements the "schedule first" approach:
 * - **Advantage**: Next run is guaranteed even if current run fails
 * - **Disadvantage**: May cause overlapping runs if execution time > periodicity
 *
 * Alternative "schedule after" approaches are possible using [tryFinallyStep] and custom context
 * management, but require more complex timing calculations.
 *
 * ## Usage Example
 *
 * ```kotlin
 * saga("health-monitor") {
 *     step { scope, message ->
 *         val healthStatus = checkSystemHealth()
 *         if (!healthStatus.healthy) {
 *             scope.launch("alert-topic", healthStatus.toJson())
 *         }
 *     }
 *     periodic("health-check", Duration.ofMinutes(5), runCount = 288) // Run for 24 hours
 * }
 * ```
 *
 * @param name Base name for the periodic steps (gets "(sleep)" and "(launch next)" suffixes)
 * @param runEvery Duration to wait between executions
 * @param runCount Maximum number of times to execute (0-based counter)
 */
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

/**
 * Creates a periodic task with an auto-generated name based on the current step count.
 *
 * This is a convenience overload of [periodic] that automatically generates a base name using the
 * current step index.
 *
 * @param runEvery Duration to wait between executions
 * @param runCount Maximum number of times to execute
 * @see periodic for detailed behavior description
 */
fun SagaBuilder.periodic(runEvery: Duration, runCount: Int) =
    periodic(steps.size.toString(), runEvery, runCount)

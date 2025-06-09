package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

/** Context key for happy path deadline tokens. */
data object HappyPathDeadlineKey : CooperationContext.MappedKey<HappyPathDeadline>()

/**
 * Deadline that applies only to normal (happy path) saga execution.
 *
 * This cancellation token causes sagas to be canceled if they exceed the specified deadline while
 * executing normal steps. It does NOT apply during rollback execution, allowing cleanup operations
 * to continue even if the original operation timed out.
 *
 * ## Usage in EventLoopStrategy
 *
 * Used by
 * [giveUpOnHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnHappyPath]
 * via
 * [happyPathDeadlineMissed][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.happyPathDeadlineMissed]
 * to determine when to cancel normal execution.
 *
 * ## Deadline Combination (and() method)
 *
 * When multiple happy path deadlines are combined, the **earliest deadline wins**:
 * - The resulting deadline uses the earlier timestamp
 * - The source is preserved from the earlier deadline
 * - All deadline information is preserved in the trace for debugging
 *
 * ## Trace Information
 *
 * The `trace` field maintains a complete history of all happy path deadlines that were combined to
 * produce the current deadline. When a saga times out, trace reveals which component/layer
 * originally set the winning (most restrictive) deadline.
 *
 * Example trace scenario:
 * ```kotlin
 * // Component A sets 5-minute deadline
 * val deadlineA = happyPathTimeout(Duration.ofMinutes(5), "component-A")
 *
 * // Component B sets 3-minute deadline
 * val deadlineB = happyPathTimeout(Duration.ofMinutes(3), "component-B")
 *
 * // Combined deadline has 3-minute timeout (from B) with A's deadline in trace
 * val combined = deadlineA + deadlineB
 * // combined.deadline = 3 minutes from now
 * // combined.source = "component-B"
 * // combined.trace = [HappyPathDeadline(5 minutes from now, "component-A")]
 * ```
 *
 * ## Common Usage Patterns
 *
 * ```kotlin
 * // Set a timeout for an entire saga
 * scope.context += happyPathTimeout(Duration.ofMinutes(5), "user-request")
 *
 * // Child operations inherit and can set more restrictive deadlines
 * scope.launch("child-topic", message, happyPathTimeout(Duration.ofMinutes(2), "child-op"))
 * ```
 *
 * @param deadline The absolute time when this saga should be cancelled
 * @param source Human-readable description of what set this deadline (for debugging)
 * @param trace History of deadline combinations that led to this deadline
 */
data class HappyPathDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<HappyPathDeadline> = emptySet(),
) : CancellationToken<HappyPathDeadline>(HappyPathDeadlineKey) {
    /**
     * Combines two happy path deadlines by choosing the earlier (more restrictive) one.
     *
     * The combination preserves the source information from the earlier deadline and maintains a
     * complete trace of all deadlines that contributed to the final result.
     *
     * @param other The other deadline to combine with
     * @return Combined deadline with the earlier timestamp and complete trace
     */
    override fun and(other: HappyPathDeadline): CancellationToken<HappyPathDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return HappyPathDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    /** Converts this deadline to a trace entry, including its own trace history. */
    private fun asTrace(): Set<HappyPathDeadline> = buildSet {
        add(HappyPathDeadline(deadline, source))
        addAll(trace)
    }
}

/**
 * Creates a happy path deadline based on a relative timeout from now.
 *
 * This is the most common way to create deadlines, specifying how long from the current time the
 * saga should be allowed to run.
 *
 * @param timeout How long from now the deadline should be
 * @param source Description of what is setting this deadline
 * @return Happy path deadline set to timeout from current time
 */
fun happyPathTimeout(timeout: Duration, source: String): HappyPathDeadline =
    HappyPathDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

/**
 * Creates a happy path deadline that never expires (set to maximum PostgreSQL timestamp).
 *
 * Used when you want to explicitly indicate no timeout rather than omitting the deadline entirely.
 *
 * @param source Description of what is setting this "no deadline" policy
 * @return Happy path deadline that will never be reached
 */
fun noHappyPathTimeout(source: String): HappyPathDeadline =
    HappyPathDeadline(postgresMaxTime, source)

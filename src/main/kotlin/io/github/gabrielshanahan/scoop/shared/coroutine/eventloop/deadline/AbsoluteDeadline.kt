package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

/** Context key for absolute deadline tokens. */
data object AbsoluteDeadlineKey : CooperationContext.MappedKey<AbsoluteDeadline>()

/**
 * Deadline that applies to the entire saga lifecycle regardless of execution phase.
 *
 * This cancellation token causes sagas to be canceled if they exceed the specified deadline during
 * ANY phase of execution - whether normal execution or rollback. It provides an overall time limit
 * for the entire saga lifecycle.
 *
 * ## Usage in EventLoopStrategy
 *
 * Used by both:
 * - [giveUpOnHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnHappyPath]
 * - [giveUpOnRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnRollbackPath]
 *
 * via
 * [absoluteDeadlineMissed][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.absoluteDeadlineMissed]
 * to provide an overall time limit regardless of execution phase.
 *
 * ## Relationship to Other Deadlines
 *
 * AbsoluteDeadline works in conjunction with path-specific deadlines:
 * - **HappyPathDeadline**: Time limit for normal execution only
 * - **RollbackPathDeadline**: Time limit for rollback execution only
 * - **AbsoluteDeadline**: Overall time limit for the entire saga
 *
 * A saga will be cancelled when ANY applicable deadline is exceeded. For example, during normal
 * execution, both HappyPathDeadline and AbsoluteDeadline are checked. During rollback, both
 * RollbackPathDeadline and AbsoluteDeadline are checked.
 *
 * ## Common Usage Patterns
 *
 * ```kotlin
 * // Set overall saga timeout (applies to both happy path and rollback)
 * scope.context += absoluteTimeout(Duration.ofHours(2), "max-saga-time")
 *
 * // Can be combined with path-specific deadlines
 * scope.context += happyPathTimeout(Duration.ofMinutes(30), "normal-ops")
 * scope.context += rollbackPathTimeout(Duration.ofMinutes(45), "cleanup-ops")
 * scope.context += absoluteTimeout(Duration.ofHours(1), "total-limit")
 * ```
 *
 * ## Deadline Combination (and() method)
 *
 * When multiple absolute deadlines are combined, the **earliest deadline wins**:
 * - The resulting deadline uses the earlier timestamp
 * - The source is preserved from the earlier deadline
 * - All deadline information is preserved in the trace for debugging
 *
 * ## Trace Information
 *
 * The `trace` field maintains a complete history of all absolute deadlines that were combined to
 * produce the current deadline. When a saga times out, trace reveals which component/layer
 * originally set the winning (most restrictive) deadline.
 *
 * Example trace with multiple deadline sources:
 * ```kotlin
 * // User SLA requires completion within 1 hour
 * val userSla = absoluteTimeout(Duration.ofHours(1), "user-sla-requirement")
 *
 * // System maintenance window allows 2 hours
 * val maintenance = absoluteTimeout(Duration.ofHours(2), "maintenance-window")
 *
 * // External service timeout is 30 minutes
 * val external = absoluteTimeout(Duration.ofMinutes(30), "external-service-limit")
 *
 * // Combined: 30-minute limit with other deadlines in trace
 * val combined = userSla + maintenance + external
 * // If saga times out, trace shows all three attempted deadline policies
 * ```
 *
 * @param deadline The absolute time when this saga should be cancelled
 * @param source Human-readable description of what set this deadline (for debugging)
 * @param trace History of deadline combinations that led to this deadline
 */
data class AbsoluteDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<AbsoluteDeadline> = emptySet(),
) : CancellationToken<AbsoluteDeadline>(AbsoluteDeadlineKey) {
    /**
     * Combines two absolute deadlines by choosing the earlier (more restrictive) one.
     *
     * The combination preserves the source information from the earlier deadline and maintains a
     * complete trace of all deadlines that contributed to the final result.
     *
     * @param other The other deadline to combine with
     * @return Combined deadline with the earlier timestamp and complete trace
     */
    override fun and(other: AbsoluteDeadline): CancellationToken<AbsoluteDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return AbsoluteDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    /** Converts this deadline to a trace entry, including its own trace history. */
    private fun asTrace(): Set<AbsoluteDeadline> = buildSet {
        add(AbsoluteDeadline(deadline, source))
        addAll(trace)
    }
}

/**
 * Creates an absolute deadline based on a relative timeout from now.
 *
 * This is the most common way to create absolute deadlines, specifying the total time allowed for
 * the entire saga lifecycle (both normal and rollback execution).
 *
 * @param timeout How long from now the deadline should be
 * @param source Description of what is setting this deadline
 * @return Absolute deadline set to timeout from current time
 */
fun absoluteTimeout(timeout: Duration, source: String): AbsoluteDeadline =
    AbsoluteDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

/**
 * Creates an absolute deadline that never expires (set to maximum PostgreSQL timestamp).
 *
 * Used when sagas should never be cancelled due to overall time limits, typically for critical
 * operations that must complete regardless of time.
 *
 * @param source Description of what is setting this "no deadline" policy
 * @return Absolute deadline that will never be reached
 */
fun noAbsoluteTimeout(source: String): AbsoluteDeadline = AbsoluteDeadline(postgresMaxTime, source)

package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline

import io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.postgresMaxTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration

/** Context key for rollback path deadline tokens. */
data object RollbackPathDeadlineKey : CooperationContext.MappedKey<RollbackPathDeadline>()

/**
 * Deadline that applies only to rollback (compensating action) execution.
 *
 * This cancellation token causes sagas to be cancelled if they exceed the specified deadline while
 * executing rollback steps. It does NOT apply during normal execution, allowing the initial
 * operation to complete even if rollback time limits are tight.
 *
 * ## Usage in EventLoopStrategy
 *
 * Used by
 * [giveUpOnRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnRollbackPath]
 * via
 * [rollbackDeadlineMissed][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.rollbackDeadlineMissed]
 * to determine when to cancel rollback execution.
 *
 * ## Deadline Combination (and() method)
 *
 * When multiple rollback deadlines are combined, the **earliest deadline wins**:
 * - The resulting deadline uses the earlier timestamp
 * - The source is preserved from the earlier deadline
 * - All deadline information is preserved in the trace for debugging
 *
 * ## Trace Information
 *
 * The `trace` field maintains a complete history of all rollback path deadlines that were combined
 * to produce the current deadline. When a saga times out, trace reveals which component/layer
 * originally set the winning (most restrictive) deadline.
 *
 * Example trace during rollback:
 * ```kotlin
 * // Database cleanup allows 10 minutes
 * val dbDeadline = rollbackPathTimeout(Duration.ofMinutes(10), "database-cleanup")
 *
 * // External API cleanup allows 5 minutes
 * val apiDeadline = rollbackPathTimeout(Duration.ofMinutes(5), "external-api-cleanup")
 *
 * // Combined: 5-minute limit with database deadline in trace
 * val combined = dbDeadline + apiDeadline
 * // If rollback times out, trace shows both attempted cleanup deadlines
 * ```
 *
 * ## Common Usage Patterns
 *
 * ```kotlin
 * // Set generous timeout for rollback operations
 * scope.context += rollbackPathTimeout(Duration.ofMinutes(30), "cleanup-timeout")
 *
 * // External service rollbacks may need even more time
 * scope.launch("external-rollback", message,
 *     rollbackPathTimeout(Duration.ofHours(1), "external-system-recovery"))
 * ```
 *
 * @param deadline The absolute time when rollback should be cancelled
 * @param source Human-readable description of what set this deadline (for debugging)
 * @param trace History of deadline combinations that led to this deadline
 */
data class RollbackPathDeadline(
    val deadline: OffsetDateTime,
    val source: String,
    val trace: Set<RollbackPathDeadline> = emptySet(),
) : CancellationToken<RollbackPathDeadline>(RollbackPathDeadlineKey) {
    /**
     * Combines two rollback deadlines by choosing the earlier (more restrictive) one.
     *
     * The combination preserves the source information from the earlier deadline and maintains a
     * complete trace of all deadlines that contributed to the final result.
     *
     * @param other The other deadline to combine with
     * @return Combined deadline with the earlier timestamp and complete trace
     */
    override fun and(other: RollbackPathDeadline): CancellationToken<RollbackPathDeadline> {
        check(key == other.key) { "Trying to mix together $key and ${other.key}" }

        val earlierDeadline = minOf(this, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline == this) other else this
        return RollbackPathDeadline(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }

    /** Converts this deadline to a trace entry, including its own trace history. */
    private fun asTrace(): Set<RollbackPathDeadline> = buildSet {
        add(RollbackPathDeadline(deadline, source))
        addAll(trace)
    }
}

/**
 * Creates a rollback path deadline based on a relative timeout from now.
 *
 * This is the most common way to create rollback deadlines, specifying how long rollback operations
 * should be allowed to run.
 *
 * @param timeout How long from now the rollback deadline should be
 * @param source Description of what is setting this deadline
 * @return Rollback deadline set to timeout from current time
 */
fun rollbackPathTimeout(timeout: Duration, source: String): RollbackPathDeadline =
    RollbackPathDeadline(
        OffsetDateTime.now().plus(timeout.inWholeMicroseconds, ChronoUnit.MICROS),
        source,
    )

/**
 * Creates a rollback deadline that never expires (set to maximum PostgreSQL timestamp).
 *
 * Used when rollback operations should never be cancelled due to time limits, typically for
 * critical cleanup operations.
 *
 * @param source Description of what is setting this "no deadline" policy
 * @return Rollback deadline that will never be reached
 */
fun noRollbackPathTimeout(source: String): RollbackPathDeadline =
    RollbackPathDeadline(postgresMaxTime, source)

package io.github.gabrielshanahan.scoop.coroutine.eventloop.deadline

import java.time.OffsetDateTime

/**
 * Common interface for deadline cancellation tokens.
 *
 * All deadline types (AbsoluteDeadline, HappyPathDeadline, RollbackPathDeadline) share the same
 * structure and combination logic. This interface extracts that common behavior while maintaining
 * type safety for each concrete deadline type.
 *
 * ## Deadline Combination
 *
 * When multiple deadlines of the same type are combined, the **earliest deadline wins**:
 * - The resulting deadline uses the earlier timestamp
 * - The source is preserved from the earlier deadline
 * - All deadline information is preserved in the trace for debugging
 *
 * ## Trace Information
 *
 * The `trace` field maintains a complete history of all deadlines that were combined to produce the
 * current deadline. When a saga times out, the trace reveals which component/layer originally set
 * the winning (most restrictive) deadline.
 *
 * @param T The concrete deadline type (for type-safe self-referential operations)
 */
interface Deadline<T : Deadline<T>> {
    /** The absolute time when this deadline expires. */
    val deadline: OffsetDateTime

    /** Human-readable description of what set this deadline (for debugging). */
    val source: String

    /** History of deadline combinations that led to this deadline. */
    val trace: Set<T>

    /**
     * Creates a new instance of the same deadline type with the given parameters.
     *
     * This factory method enables the shared combination logic to create new instances of the
     * concrete type without knowing the specific class.
     */
    fun create(deadline: OffsetDateTime, source: String, trace: Set<T>): T

    /**
     * Creates a copy of this deadline without its trace history.
     *
     * Used when adding this deadline to another deadline's trace.
     */
    fun withoutTrace(): T = create(deadline, source, emptySet())

    /**
     * Converts this deadline to a trace entry, including its own trace history.
     *
     * The result is a set containing this deadline (without its trace) plus all deadlines from this
     * deadline's trace. This flattens the trace hierarchy into a single set.
     */
    fun asTrace(): Set<T> = buildSet {
        add(withoutTrace())
        addAll(trace)
    }

    /**
     * Combines two deadlines by choosing the earlier (more restrictive) one.
     *
     * The combination preserves the source information from the earlier deadline and maintains a
     * complete trace of all deadlines that contributed to the final result.
     *
     * @param other The other deadline to combine with
     * @return Combined deadline with the earlier timestamp and complete trace
     */
    fun combineWith(other: T): T {
        @Suppress("UNCHECKED_CAST") val self = this as T
        val earlierDeadline = minOf(self, other, compareBy { it.deadline })
        val laterDeadline = if (earlierDeadline === self) other else self
        return create(
            earlierDeadline.deadline,
            earlierDeadline.source,
            earlierDeadline.trace + laterDeadline.asTrace(),
        )
    }
}

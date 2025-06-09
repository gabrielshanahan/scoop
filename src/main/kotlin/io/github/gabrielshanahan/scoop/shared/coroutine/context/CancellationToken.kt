package io.github.gabrielshanahan.scoop.shared.coroutine.context

/**
 * Base class for context elements that can cause saga cancellation.
 *
 * CancellationToken is a generic context element framework used by
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
 * implementations to determine when sagas should be cancelled. The most common implementation is
 * deadlines, but the framework is extensible for other cancellation criteria.
 *
 * ## Distinction from CANCELLATION_REQUESTED
 *
 * **Important**: CancellationToken represents *automatic* cancellation based on system-defined
 * criteria (timeouts, resource limits, etc.). This is completely separate from
 * `CANCELLATION_REQUESTED` message events, which represent *user-initiated* cancellation (e.g.,
 * someone clicks a cancel button). See
 * [StructuredCooperationCapabilities.cancel][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationCapabilities.cancel].
 *
 * ## Custom plus() Logic
 *
 * The [plus] method implements special combining logic for cancellation tokens:
 * - When adding two tokens of the same type, they are combined using [and]
 * - This enables "most restrictive wins" semantics for deadlines and similar criteria
 * - Different token types follow normal [CooperationContext] combining rules
 *
 * ## Usage in EventLoopStrategy
 *
 * CancellationToken instances are used in EventLoopStrategy methods:
 * - [giveUpOnHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnHappyPath] -
 *   checks tokens to cancel normal execution
 * - [giveUpOnRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnRollbackPath] -
 *   checks tokens to cancel rollback execution
 *
 * The strategy generates SQL queries that examine the saga's CooperationContext for cancellation
 * tokens and determine if cancellation criteria have been met. Note: it's never enough to just
 * implement an instance of [CancellationToken], you always need to also add support in some
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy].
 *
 * ## Provided Implementations
 * - [HappyPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.HappyPathDeadline] -
 *   Timeout for normal execution
 * - [RollbackPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.RollbackPathDeadline] -
 *   Timeout for rollback execution
 * - [AbsoluteDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.AbsoluteDeadline] -
 *   Overall saga timeout
 *
 * ## Implementation Example
 *
 * ```kotlin
 * data class ResourceQuotaExceeded(
 *     val quotaType: String,
 *     val limit: Int
 * ) : CancellationToken<ResourceQuotaExceeded>(ResourceQuotaKey) {
 *     override fun and(other: ResourceQuotaExceeded): CancellationToken<ResourceQuotaExceeded> {
 *         // Most restrictive quota wins
 *         return if (this.limit <= other.limit) this else other
 *     }
 * }
 * ```
 *
 * @param SELF The concrete type of this cancellation token for type-safe combining
 * @param key The CooperationContext key for this token type
 */
abstract class CancellationToken<SELF : CancellationToken<SELF>>(
    key: CooperationContext.MappedKey<*>
) : CooperationContext.MappedElement(key) {
    /**
     * Custom combining logic for cancellation tokens.
     *
     * When two cancellation tokens of the same type are combined, they are merged using the [and]
     * method. This enables "most restrictive wins" behavior for deadlines and similar criteria.
     *
     * For tokens of different types, normal CooperationContext combining applies.
     *
     * @param context The context to combine with this token
     * @return Combined context with appropriate token merging
     */
    override fun plus(context: CooperationContext): CooperationContext =
        if (context is CancellationToken<SELF> && context.has(key)) {
            @Suppress("UNCHECKED_CAST") and(context as SELF)
        } else {
            super.plus(context)
        }

    /**
     * Combines two cancellation tokens of the same type.
     *
     * Implementations should decide how to merge two tokens, typically using "most restrictive
     * wins" logic. For deadlines, this means choosing the earlier deadline. For quotas, this means
     * choosing the lower limit.
     *
     * @param other The other cancellation token to combine with
     * @return The combined cancellation token
     */
    abstract fun and(other: SELF): CancellationToken<SELF>
}

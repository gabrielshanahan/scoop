package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy

/**
 * Represents a single step in a distributed saga.
 * 
 * Each step in a saga corresponds to a single database transaction. A step can:
 * 1. Perform business logic (via [invoke])
 * 2. Handle failures from child handlers (via [handleChildFailures])  
 * 3. Compensate for its actions during rollback (via [rollback])
 * 
 * Steps are the fundamental building blocks of structured cooperation. The core rule of
 * structured cooperation is that a saga suspends after completing a step and doesn't
 * continue to the next step until all handlers of messages emitted in that step have finished.
 * 
 * For more details on structured cooperation, see: 
 * https://developer.porn/posts/introducing-structured-cooperation/
 */
interface TransactionalStep {
    /**
     * The unique name of this step within the saga.
     * 
     * Step names are used for tracking execution state and building continuations. 
     * They must be unique within a single [DistributedCoroutine].
     */
    val name: String

    /**
     * Executes the main business logic of this step.
     * 
     * This method runs within a database transaction. Any messages emitted via the [scope]
     * during this method's execution will be committed atomically with any database changes.
     * 
     * After this method completes successfully, the saga will suspend and wait for all
     * handlers of emitted messages to complete before proceeding to the next step. This
     * is the core mechanism that implements structured cooperation.
     * 
     * @param scope The cooperation scope, providing access to message emission and context
     * @param message The message that triggered this saga (for the first step) or that
     *                caused this step to resume
     */
    fun invoke(scope: CooperationScope, message: Message)

    /**
     * Defines the compensating action for this step during rollback.
     * 
     * When a saga needs to roll back (due to an exception in a later step or child handler),
     * this method is called to "undo" the effects of the [invoke] method. This runs in its
     * own database transaction.
     * 
     * The default implementation does nothing, which is appropriate for read-only operations
     * or operations that don't require compensation.
     * 
     * @param scope The cooperation scope (same as during [invoke])
     * @param message The original message that triggered this step
     * @param throwable The exception that caused the rollback to begin
     *        
     * TODO: Isn't this always an instance of CooperationException, and not Throwable?
     */
    fun rollback(scope: CooperationScope, message: Message, throwable: Throwable) = Unit

    /**
     * Handles failures from child message handlers spawned by this step.
     * 
     * When a message handler (child saga) fails, this method is called before the saga
     * enters rollback mode. This allows the parent saga to:
     * - Retry the operation
     * - Handle the error gracefully and continue
     * - Transform the error into a different exception
     * - Deliberately ignore certain types of errors
     * 
     * **Important considerations:**
     * - This method is conceptually part of the same step as [invoke], even though it
     *   runs in a separate transaction
     * - No guarantees are made about rollback order (all ROLLBACK_EMITTED events 
     *   are processed concurrently)
     * - Any [rollback] implementation must account for the fact that [handleChildFailures]
     *   already ran and should compensate for those effects too
     * - This is why structured cooperation doesn't provide retry mechanisms out of the box
     * 
     * The default implementation re-throws the exception, causing the saga to enter
     * rollback mode.
     * 
     * @param scope The cooperation scope
     * @param message The original message that triggered this step  
     * @param throwable The exception from the child handler that failed
     * @throws Throwable If the error should cause this saga to roll back
     * 
     * **Important**: Whatever happens in [handleChildFailures] is conceptually part of the same
     * step as [invoke], even though it runs in a different transaction. This has implications:
     * 
     * - **No rollback order guarantees**: All ROLLBACK_EMITTED events are processed concurrently
     * - **Rollback considerations**: Any [rollback] implementation must account for [handleChildFailures]
     *   having already run and should compensate for those effects too
     * - **Use cases**: Best suited for simple retries or deliberately ignoring specific errors
     * - **Why no built-in retry**: This complexity is why structured cooperation doesn't provide
     *   retry mechanisms out of the box
     * 
     * **Future improvement**: A cleaner approach would be to have a separate rollback step for
     * [handleChildFailures], but this would require supporting dynamic "micro-steps" between
     * each step, which is beyond the current implementation scope.
     */
    fun handleChildFailures(scope: CooperationScope, message: Message, throwable: Throwable): Unit =
        throw throwable
}

/**
 * Represents a distributed saga - a sequence of [TransactionalStep]s that implement structured cooperation.
 * 
 * A [DistributedCoroutine] is the core abstraction in Scoop that represents a message handler.
 * It defines a sequence of steps that are executed one after another, with the crucial property
 * that execution suspends after each step until all child message handlers have completed.
 * 
 * This suspension-and-resume behavior is what enables structured cooperation's key benefits:
 * - **Distributed consistency**: The system is in a consistent state before proceeding
 * - **Distributed exceptions**: Failures can propagate up the call hierarchy
 * - **Distributed resource management**: Resources can be properly cleaned up via rollback
 * 
 * For conceptual background, see: https://developer.porn/posts/introducing-structured-cooperation/
 * For implementation details, see: https://developer.porn/posts/implementing-structured-cooperation/
 * 
 * @param identifier Unique identifier for this coroutine type (used for handler registration)
 * @param steps The sequence of steps that make up this saga. Must be non-empty and have unique names.
 * @param eventLoopStrategy Strategy for determining when this coroutine should run (see [EventLoopStrategy])
 */
class DistributedCoroutine(
    val identifier: DistributedCoroutineIdentifier,
    val steps: List<TransactionalStep>,
    val eventLoopStrategy: EventLoopStrategy,
) {
    init {
        check(steps.isNotEmpty()) { "Steps cannot be empty" }
        check(steps.mapTo(mutableSetOf()) { it.name }.size == steps.size) {
            "Steps must have unique names"
        }
    }
}

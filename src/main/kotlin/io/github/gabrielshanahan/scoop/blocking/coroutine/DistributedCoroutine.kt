package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy

/**
 * Represents a single step in a distributed saga, which is represented as a [DistributedCoroutine].
 * You will rarely want to build instances of these objects manually - instead, use
 * [SagaBuilder][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SagaBuilder].
 *
 * Each step in a saga corresponds to a single database transaction. A step can:
 * 1. Perform business logic (via [invoke])
 * 2. Handle failures from child handlers (via [handleChildFailures])
 * 3. Compensate for its actions during rollback (via [rollback])
 *
 * ## Child Handlers and Parent-Child Relationships
 *
 * In structured cooperation, when a step emits messages using `scope.launch()`, the handlers of
 * those messages are called "child handlers" (or "child sagas") of this saga. This parent-child
 * relationship is tracked using something called
 * [cooperationLineage][io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier.cooperationLineage] -
 * basically, a list of UUIDs, which each "child" emission inherits from its parent, appends its own
 * UUID to, and passes on to *its* children.
 *
 * Some key ways this parent-child relationship is used:
 * - **Parent waits for children**: The saga suspends after this step and won't proceed until ALL
 *   child handlers have completed (successfully or via rollback). This is actually encapsulated by
 *   the default instance of [EventLoopStrategy] (see
 *   [StandardEventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.StandardEventLoopStrategy]).
 *   However, you could, conceivably, implement any other rule you like, like, e.g.,
 *   [SleepEventLoopStrategy][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SleepEventLoopStrategy]
 *   does.
 * - **Child failures propagate up**: If any child handler fails, those failures bubble up to this
 *   parent step, and into [handleChildFailures]. See [EventLoop] and
 *   [BaseCooperationContinuation.resumeWith][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.BaseCooperationContinuation.resumeWith].
 * - **Rollback propagates down**: If a step needs to roll back, rollback messages are first sent to
 *   all child handlers, and only after they have finished rolling back does this step's [rollback]
 *   method runs. See
 *   [RollbackPathContinuation][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.RollbackPathContinuation].
 *
 * This parent-child hierarchy is key for implementing structured cooperation, whose core rule is:
 * **a saga suspends after completing a step and doesn't continue to the next step until all
 * handlers of messages emitted in that step have finished.**
 *
 * For more details on structured cooperation, see:
 * https://developer.porn/posts/introducing-structured-cooperation/
 */
interface TransactionalStep {
    /**
     * The unique name of this step within the saga.
     *
     * Step names are used for tracking execution state and building continuations. They must be
     * unique within a single [DistributedCoroutine].
     */
    val name: String

    /**
     * Executes the main business logic of this step.
     *
     * This method runs within a database transaction. Any database operations executed using the
     * [scope]'s [connection][CooperationScope.connection] during this method's execution will be
     * committed atomically when the step finishes executing (or rolled back if an exception
     * happens). This includes emitting messages via [CooperationScope.launch] and
     * [CooperationScope.launchOnGlobalScope].
     *
     * After this method completes successfully, the saga will suspend and wait for the associated
     * [DistributedCoroutine.eventLoopStrategy] to signal that it should resume - typically, this is
     * implemented as waiting for all handlers of all emitted messages to complete, which is the
     * core mechanism that implements structured cooperation.
     *
     * @param scope The cooperation scope, providing access to message emission and context
     * @param message The message that triggered this saga (for the first step) or that caused this
     *   step to resume
     */
    fun invoke(scope: CooperationScope, message: Message)

    /**
     * Defines the compensating action for this step during rollback.
     *
     * When a saga needs to roll back (due to an exception in a later step or child handler), this
     * method is called to "undo" the effects of the [invoke] method. This runs in its own database
     * transaction.
     *
     * The default implementation does nothing.
     *
     * @param scope The cooperation scope (same as during [invoke])
     * @param message The original message that triggered this step
     * @param throwable The exception that caused the rollback to begin
     */
    fun rollback(scope: CooperationScope, message: Message, throwable: Throwable) = Unit

    /**
     * Handles failures from child message handlers spawned by this step.
     *
     * When a child message handler (child saga) fails, this method is called before the saga enters
     * rollback mode. This allows the parent saga to:
     * - Retry the operation
     * - Handle the error gracefully and continue
     * - Transform the error into a different exception
     * - Deliberately ignore certain types of errors
     *
     * **Important considerations:**
     * - This method is conceptually part of the same step as [invoke], even though it runs in a
     *   separate transaction. That means that any messages emitted during its execution will be
     *   treated as if they were emitted from [invoke].
     * - This applies mainly to rollbacks, where one might expect rollbacks to first happen for
     *   messages emitted from [handleChildFailures], and only then for messages emitted from
     *   [invoke]. However, in the current POC implementation, no guarantees are made about rollback
     *   order, and all ROLLBACK_EMITTED events are emitted concurrently.
     * - Any [rollback] implementation must account for the fact that [handleChildFailures] already
     *   ran and should compensate for those effects too
     * - This is the main reason why Scoop doesn't provide a retry abstraction, since this needs to
     *   be accounted for manually
     *
     * The default implementation re-throws the exception, causing the saga to enter rollback mode.
     *
     * **Future improvement**: A cleaner approach would be to have a separate rollback step for
     * anything emitted from [handleChildFailures], but this would require supporting a dynamic
     * number "semi-steps" between each step (has [handleChildFailures] can run multiple times),
     * which is definitely doable, but out of scope for a rudimentary POC.
     *
     * @param scope The cooperation scope
     * @param message The original message that triggered this step
     * @param throwable The exception from the child handler that failed
     * @throws Throwable If the error should cause this saga to roll back
     */
    fun handleChildFailures(scope: CooperationScope, message: Message, throwable: Throwable): Unit =
        throw throwable
}

/**
 * Represents a distributed saga - a sequence of [TransactionalStep]s that can be suspended and
 * resumed.
 *
 * ## Why "Coroutine"?
 *
 * A **coroutine** is a programming concept representing a function that can be suspended and
 * resumed. Unlike regular functions that run from start to finish, coroutines can pause their
 * execution at specific points and continue later from exactly where they left off.
 *
 * In Scoop, we call sagas "DistributedCoroutines" because they exhibit this same suspend-and-resume
 * behavior, but in a distributed context:
 * - **Suspension**: After each step completes and emits messages, the saga suspends execution
 * - **Resumption**: The saga resumes only when the [EventLoopStrategy] says so
 * - **State preservation**: The saga's execution state (step position, context, rollback status) is
 *   preserved in the database between suspension and resumption
 * - **Distributed**: Unlike traditional coroutines that suspend within a single process, these
 *   suspend across service boundaries and can resume on different machines
 *
 * This suspend-and-resume pattern is what enables structured cooperation's key property: ensuring
 * all child handlers complete before proceeding to the next step.
 *
 * ## Relationship to Kotlin Coroutines
 *
 * Scoop doesn't use Kotlin coroutines. While Scoop's DistributedCoroutines are conceptually similar
 * to, and strongly inspired by, Kotlin's coroutines, they operate at a much higher level. Kotlin
 * coroutines suspend within a single JVM process, while DistributedCoroutines suspend across
 * database transactions and distributed message handling.
 *
 * @param identifier Unique identifier for this coroutine type (used for handler registration)
 * @param steps The sequence of steps that make up this saga. Must be non-empty and have unique
 *   names.
 * @param eventLoopStrategy Strategy that determines various aspects of when and how this saga will
 *   execute
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

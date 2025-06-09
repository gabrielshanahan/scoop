package io.github.gabrielshanahan.scoop.reactive.coroutine.continuation

import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import io.smallrye.mutiny.Uni

/**
 * Represents the execution of a single step within a saga - a delimited continuation.
 *
 * ## What is a Continuation?
 *
 * A **Continuation** is a fundamental concept in computer science that represents "the rest of the
 * computation" from a given point. A **delimited continuation** represents only a portion of that
 * computation - bounded by specific start and end points. Think of it like a single page in a book
 * rather than "everything from here to the end".
 *
 * In Scoop, a [Continuation] is a delimited continuation that represents the next step to be
 * executed - from "just before the next step" (i.e., just after all child handlers from the last
 * step have finished) to "after that step finishes executing, just before the transaction commits".
 * The most important part of a [Continuation] is [resumeWith], which is what actually executes the
 * step in question.
 *
 * ## Delimited Nature
 *
 * Scoop's continuations are delimited because:
 * - They execute exactly **one step** of the saga
 * - After executing that step, they return control to the
 *   [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop]
 * - The EventLoop then creates a new continuation for the next step (if needed)
 * - This creates natural suspension points between each step
 *
 * ## Suspension Points in Scoop
 *
 * **Important**: Suspension points occur just AFTER a step finishes executing and emitting
 * messages, but BEFORE the next step begins. A [Continuation] represents the execution from one
 * suspension point to the next - it handles any child failures from the previous step, then
 * executes the next step, then suspends again.
 *
 * As a consequence, the span of a [Continuation] in Scoop actually touches two steps - it starts at
 * the very end of the previous step, where we deal with any child failures, and then proceeds to
 * the next step, which it executes.
 *
 * ## Execution Flow
 *
 * The [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] creates
 * continuations and resumes them with information about what happened in the previous step
 * (success, failure, or rollback completion). The continuation then:
 * 1. **Handles child failures first**: If there were failures from child handlers of the previous
 *    step, the continuation processes these through
 *    [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.handleChildFailures].
 *    If this handling fails (throws an exception), the continuation stops here and returns the
 *    failure.
 * 2. **Executes the corresponding step**: If child failure handling succeeds (or there were no
 *    child failures), executes the specific step determined by the continuation type - either the
 *    next forward step ([HappyPathContinuation]) or the next rollback step
 *    ([RollbackPathContinuation])
 * 3. **Returns control**: Provides the
 *    [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] with a result
 *    (suspend, success, or failure)
 *
 * See
 * [EventLoop.resumeCoroutine][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop.resumeCoroutine]
 * for details.
 *
 * ## Types of Continuations
 *
 * There are two main types of continuations in Scoop:
 * - [HappyPathContinuation]: For executing the next forward step in the saga
 * - [RollbackPathContinuation]: For executing the next rollback step (compensating action)
 *
 * Each continuation encapsulates the logic for executing exactly one step in its respective
 * execution mode (forward or rollback), then returns control to the EventLoop.
 *
 * ## Reactive Implementation
 *
 * In the reactive implementation, [resumeWith] returns a `Uni<ContinuationResult>` to enable
 * composition with other reactive operations. All step execution is handled asynchronously through
 * the reactive chain.
 */
interface Continuation {

    /**
     * Unique identifier for this continuation instance.
     *
     * This identifier tracks which specific saga instance and step this continuation represents.
     * It's used for logging, debugging, and correlating continuation executions with database
     * events.
     */
    val continuationIdentifier: ContinuationIdentifier

    /**
     * Resumes execution of the saga with the result from the previous step.
     *
     * This is the core method that drives saga execution. It receives information about what
     * happened in the previous step and then:
     * 1. **Handles child failures first** (if any) through
     *    [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.handleChildFailures].
     * 2. **Executes the predetermined step** based on the continuation type:
     *     - [HappyPathContinuation]: Executes the next forward step
     *     - [RollbackPathContinuation]: Executes the next rollback step (compensating action)
     *
     * The continuation doesn't dynamically determine what step to execute - that was decided when
     * the EventLoop chose which continuation type to create. The continuation either fails during
     * child failure handling or executes its predetermined step type.
     *
     * @param lastStepResult What happened in the previous step (success, failure, or rollback)
     * @return A [Uni] that completes with what should happen next (suspend, success, or failure)
     */
    fun resumeWith(lastStepResult: LastStepResult): Uni<out ContinuationResult>

    /**
     * Represents the outcome of the previous step execution.
     *
     * This is passed to [resumeWith] to inform the continuation about what happened in the previous
     * step.
     */
    sealed interface LastStepResult {
        /** The message that was being processed */
        val message: Message

        /**
         * The previous step completed successfully.
         *
         * This means the step's
         * [TransactionalStep.invoke][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.invoke]
         * method completed without throwing an exception, and all child handlers (if any) also
         * finished successfully.
         */
        data class SuccessfullyInvoked(override val message: Message) : LastStepResult

        /**
         * The saga is ready to proceed with rollback execution.
         *
         * **Important**: This does NOT necessarily mean a previous rollback step has completed!
         * This result covers two distinct scenarios:
         * 1. **Rollback step completed**: The previous step's
         *    [TransactionalStep.rollback][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.rollback]
         *    method executed and completed without throwing an exception
         * 2. **Rollback just started**: A ROLLING_BACK event was written to initiate rollback mode,
         *    and we're about to execute the first rollback step (no actual rollback step has run
         *    yet)
         *
         * However, from the continuation's perspective, both scenarios mean the same thing:
         * "proceed with executing the next rollback step." The [throwable] is the original
         * exception that caused the rollback to begin.
         */
        data class SuccessfullyRolledBack(override val message: Message, val throwable: Throwable) :
            LastStepResult

        /**
         * Child handlers from the previous step failed.
         *
         * This indicates that the previous step's
         * [TransactionalStep.invoke][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.invoke]
         * method completed successfully and committed, but one or more child handlers that were
         * spawned by emitting messages failed. The step itself did not fail - if it had, the
         * transaction would never have committed and there would be nothing to roll back.
         */
        data class Failure(override val message: Message, val throwable: Throwable) :
            LastStepResult
    }

    /**
     * Represents the outcome of resuming the continuation.
     *
     * This tells the [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] what
     * happened when the continuation was resumed and what should happen next.
     */
    sealed interface ContinuationResult {
        /**
         * The saga successfully executed a step (normal or rollback) and should now be suspended,
         * waiting for child handlers to complete (or, more generally, for the
         * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
         * to say it should resume).
         *
         * This is the normal result when a step completes successfully, regardless of whether any
         * messages were emitted.
         *
         * @param emittedMessages Messages that were emitted during this step's execution
         */
        data class Suspend(val emittedMessages: List<Message>) : ContinuationResult

        /**
         * The saga completed successfully.
         *
         * This indicates that all steps in the saga have finished executing successfully and the
         * saga has reached its natural conclusion.
         */
        data object Success : ContinuationResult

        /**
         * The step failed with an unhandled exception.
         *
         * This indicates that either:
         * - A child threw an exception that wasn't handled by
         *   [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.handleChildFailures]
         * - An unhandled exception was thrown in the step
         *   ([TransactionalStep.invoke][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.invoke]
         *   or
         *   [TransactionalStep.rollback][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.rollback])
         *
         * When this result is returned, the
         * [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] will initiate
         * rollback processing or mark the saga as failed, depending on the current execution state.
         */
        data class Failure(val exception: Throwable) : ContinuationResult
    }
}

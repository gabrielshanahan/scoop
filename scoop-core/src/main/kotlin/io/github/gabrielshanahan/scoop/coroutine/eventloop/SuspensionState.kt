package io.github.gabrielshanahan.scoop.coroutine.eventloop

import java.time.Instant

/**
 * Represents whether the last event loop tick was a child failure handler invocation.
 *
 * When children fail, the parent step's
 * [handleChildFailures][io.github.gabrielshanahan.scoop.coroutine.TransactionalStep.handleChildFailures]
 * is called. Each such invocation is assigned an incrementing iteration number so that the events
 * it produces are distinguishable from events produced by the step's normal
 * [invoke][io.github.gabrielshanahan.scoop.coroutine.TransactionalStep.invoke] execution.
 */
sealed interface ChildFailureHandlerIteration {
    /** The last tick was not a child failure handler invocation. */
    data object NoChildFailure : ChildFailureHandlerIteration

    /** The last tick was child failure handler invocation number [iteration] (0-based). */
    data class HandlerIteration(val iteration: Int) : ChildFailureHandlerIteration

    /**
     * Returns the next iteration: [NoChildFailure] becomes `HandlerIteration(0)`,
     * [HandlerIteration] increments.
     */
    fun incremented(): HandlerIteration =
        when (this) {
            is NoChildFailure -> HandlerIteration(0)
            is HandlerIteration -> HandlerIteration(iteration + 1)
        }
}

/**
 * Tracks the execution progress of a saga by indicating the current suspension state. Constructed
 * in [EventLoop][io.github.gabrielshanahan.scoop.coroutine.EventLoop], based on data retrieved by
 * [MessageEventRepository.fetchPendingCoroutineRun][io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.MessageEventRepository.fetchPendingCoroutineRun].
 *
 * This sealed interface represents the suspension state of a saga in
 * [EventLoop][io.github.gabrielshanahan.scoop.coroutine.EventLoop], enabling it to determine what
 * the next action should be.
 *
 * ## Saga Execution Flow
 *
 * Sagas progress through execution states:
 * 1. **NotSuspendedYet**: Brand new saga ready to execute its first step
 * 2. **SuspendedBetweenSteps(completedStep, nextStep)**: Saga completed a step and will execute the
 *    next step (which may be the same step for
 *    [NextStep.Repeat][io.github.gabrielshanahan.scoop.coroutine.NextStep.Repeat] or a different
 *    step for [NextStep.GoTo][io.github.gabrielshanahan.scoop.coroutine.NextStep.GoTo])
 * 3. **LastStepFinished(completedStep)**: Saga completed its final step
 * 4. **SuspendedAfterStepRollback(completedRollbackStep)**: Saga is rolling back and completed a
 *    rollback sub-step
 *
 * ## EventLoop Decision Making
 *
 * The EventLoop uses this state combined with [RollbackState] to determine actions:
 * - **(NotSuspendedYet, Gucci)**: Execute the first step
 * - **(SuspendedBetweenSteps, Gucci)**: Execute the next step
 * - **(LastStepFinished, Gucci)**: Commit the saga
 * - **(SuspendedAfterStepRollback, SuccessfullyRolledBackLastStep)**: Continue rolling back
 *   previous steps
 * - **(SuspendedBetweenSteps/LastStepFinished, ChildrenFailed...)**: Handle child failures and
 *   potentially start rollback
 *
 * @see RollbackState for the rollback status component of saga state
 */
sealed interface SuspensionState {
    /**
     * Indicates a brand new saga that hasn't executed any steps yet.
     *
     * This is the initial state for sagas that have just been created but haven't started
     * execution. The EventLoop will execute the first step when encountering this state.
     */
    data object NotSuspendedYet : SuspensionState

    /**
     * Indicates a saga that completed [completedStep] and will next execute [nextStep].
     *
     * Both steps are identified by name for consistency. On the happy path, [nextStep] may be the
     * same as [completedStep] (if
     * [NextStep.Repeat][io.github.gabrielshanahan.scoop.coroutine.NextStep.Repeat] was returned) or
     * a different step (for
     * [NextStep.Continue][io.github.gabrielshanahan.scoop.coroutine.NextStep.Continue] or
     * [NextStep.GoTo][io.github.gabrielshanahan.scoop.coroutine.NextStep.GoTo]).
     *
     * @param completedStep The name of the step that completed and caused suspension
     * @param nextStep The name of the step to execute next
     * @param suspendedAt The timestamp of the SUSPENDED event
     * @param childFailureHandlerIteration Whether the last tick was a child failure handler
     *   invocation, and if so, which iteration
     */
    data class SuspendedBetweenSteps(
        val completedStep: String,
        val nextStep: String,
        val suspendedAt: Instant,
        val childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) : SuspensionState

    /**
     * Indicates a saga that completed its final step. The saga is ready to be committed (on the
     * happy path) or has no more steps to execute before finishing.
     *
     * @param completedStep The name of the last step that completed
     * @param suspendedAt The timestamp of the SUSPENDED event
     * @param childFailureHandlerIteration Whether the last tick was a child failure handler
     *   invocation, and if so, which iteration
     */
    data class LastStepFinished(
        val completedStep: String,
        val suspendedAt: Instant,
        val childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) : SuspensionState

    /**
     * Indicates a saga that is executing rollback and completed a rollback sub-step.
     *
     * On the rollback path, step names are synthetic (e.g., "rollingBack-Step1-iteration-0") and
     * the next rollback step is determined from the rollback step sequence, not from a stored
     * next-step value. The `next_step` column in the database is always NULL for rollback SUSPENDED
     * events.
     *
     * @param completedRollbackStep The synthetic name of the rollback step that completed
     * @param suspendedAt The timestamp of the SUSPENDED event
     * @param childFailureHandlerIteration Whether the last tick was a child failure handler
     *   invocation, and if so, which iteration. Rollback steps can emit child messages via
     *   `scope.launch()`, and if those children fail, `handleChildFailures` is called on the
     *   rollback step.
     */
    data class SuspendedAfterStepRollback(
        val completedRollbackStep: String,
        val suspendedAt: Instant,
        val childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) : SuspensionState
}

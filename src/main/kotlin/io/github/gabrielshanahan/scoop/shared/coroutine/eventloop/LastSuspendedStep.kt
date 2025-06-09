package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

/**
 * Tracks the execution progress of a saga by indicating the last step that completed. Constructed
 * in [EventLoop][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop], based on data
 * retrieved by
 * [MessageEventRepository.fetchPendingCoroutineRun][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository.fetchPendingCoroutineRun].
 *
 * This sealed interface represents the suspension state of a saga in
 * [EventLoop][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop], enabling it to
 * determine what the next action should be.
 *
 * ## Saga Execution Flow
 *
 * Sagas progress through execution states:
 * 1. **NotSuspendedYet**: Brand new saga ready to execute its first step
 * 2. **SuspendedAfter(<fist step name>)**: Saga completed a step and is waiting for child handlers
 * 3. **SuspendedAfter** with the next step name, and so on
 *
 * ## EventLoop Decision Making
 *
 * The EventLoop uses this state combined with [RollbackState] to determine actions:
 * - **(NotSuspendedYet, Gucci)**: Execute the first step
 * - **(SuspendedAfter, Gucci)**: Execute the next step
 * - **(SuspendedAfter, SuccessfullyRolledBackLastStep)**: Continue rolling back previous steps
 * - **(SuspendedAfter, ChildrenFailed...)**: Handle child failures and potentially start rollback
 *
 * @see RollbackState for the rollback status component of saga state
 */
sealed interface LastSuspendedStep {
    /**
     * Indicates a brand new saga that hasn't executed any steps yet.
     *
     * This is the initial state for sagas that have just been created but haven't started
     * execution. The EventLoop will execute the first step when encountering this state.
     */
    data object NotSuspendedYet : LastSuspendedStep

    /**
     * Indicates a saga that completed [stepName].
     *
     * @param stepName The name of the step that completed and caused suspension
     */
    data class SuspendedAfter(val stepName: String) : LastSuspendedStep
}

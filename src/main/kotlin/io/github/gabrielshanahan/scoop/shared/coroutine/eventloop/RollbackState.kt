package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException

/**
 * Represents the rollback status of a saga, tracking both self and child rollback states.
 * Constructed in [EventLoop][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop], based
 * on data retrieved by
 * [MessageEventRepository.fetchPendingCoroutineRun][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository.fetchPendingCoroutineRun].
 *
 * RollbackState forms a type hierarchy that captures all possible combinations of rollback
 * scenarios in distributed saga execution. It tracks two orthogonal dimensions:
 * - **Me**: Rollback status of this saga itself
 * - **Children**: Rollback status of child handlers
 *
 * ## Type Hierarchy Design
 *
 * The hierarchy uses intersection types to represent all possible combinations:
 * ```
 * RollbackState
 * ├── NoThrowable (no exceptions present)
 * ├── ThrowableExists (exceptions present)
 * │
 * │
 * ├── Me (this saga's rollback status)
 * │   ├── NotRollingBack (saga not currently rolling back) : NoThrowable
 * │   └── RollingBack (saga actively rolling back) : ThrowableExists
 * └── Children (child handlers' rollback status)
 *     ├── NoRollbacks (no child rollback issues) : NoThrowable
 *     └── Rollbacks (child rollback issues exist) : ThrowableExists
 *         ├── Successful (children rolled back successfully)
 *         └── Failed (children failed to roll back)
 * ```
 *
 * ## State Combinations
 *
 * Each concrete implementation represents a specific combination:
 * - **Gucci**: Everything is fine (Me.NotRollingBack + Children.NoRollbacks)
 * - **SuccessfullyRolledBackLastStep**: This saga rolled back, no child issues
 * - **ChildrenFailedWhileRollingBackLastStep**: This saga rolling back + children failed rollback
 * - **ChildrenFailedAndSuccessfullyRolledBack**: Children failed, successfully rolled back + this
 *   saga not rolling back
 * - **ChildrenFailedAndFailedToRollBack**: Children failed and also failed rollback + this saga not
 *   rolling back
 *
 * ## EventLoop Decision Making
 *
 * The EventLoop uses RollbackState with [LastSuspendedStep] to determine actions:
 * - **Gucci**: Continue normal execution
 * - **SuccessfullyRolledBackLastStep**: Continue rolling back previous steps
 * - **Children failed states**: Handle child failures (potentially triggering rollback, or
 *   terminating with `ROLLBACK_FAILED`)
 *
 * ## Exception Precedence
 *
 * **Important**: Unlike Java's suppressed exceptions, rollback failures take precedence over
 * original exceptions. If both original operation and rollback fail, the rollback failure becomes
 * the primary exception with the original as a cause.
 *
 * @see LastSuspendedStep for the execution progress component of saga state
 */
sealed interface RollbackState {

    /**
     * Marker interface for rollback states where no exceptions are present.
     *
     * This indicates a "clean" state where neither this saga nor its children have encountered any
     * failures that require exception handling.
     */
    sealed interface NoThrowable : RollbackState

    /**
     * Marker interface for rollback states where exceptions are present.
     *
     * This indicates that either this saga, its children, or both have encountered failures that
     * resulted in exceptions needing to be handled.
     *
     * @property throwable The exception that needs to be propagated or handled
     */
    sealed interface ThrowableExists : RollbackState {
        val throwable: Throwable
    }

    /**
     * Represents this saga's own rollback status (not considering children).
     *
     * This dimension tracks whether the current saga is actively executing rollback steps or is in
     * normal execution mode.
     */
    sealed interface Me : RollbackState {
        sealed interface NotRollingBack : Me, NoThrowable

        sealed interface RollingBack : Me, ThrowableExists
    }

    /**
     * Represents the rollback status of child handlers spawned by this saga.
     *
     * This dimension tracks whether child handlers have experienced failures and how their rollback
     * attempts have proceeded.
     */
    sealed interface Children : RollbackState {
        /** No child handlers have experienced rollback issues. */
        sealed interface NoRollbacks : Children, NoThrowable

        /**
         * Child handlers have experienced rollback scenarios.
         *
         * At least one child handler has failed, and rollback attempts have been made.
         */
        sealed interface Rollbacks : Children, ThrowableExists {
            /**
             * Child rollbacks completed successfully.
             *
             * Children failed initially, but their compensating actions executed successfully.
             */
            sealed interface Successful : Rollbacks

            /**
             * Child rollbacks failed.
             *
             * Children failed initially, and their compensating actions also failed.
             */
            sealed interface Failed : Rollbacks
        }
    }

    /**
     * Everything is working perfectly - no rollback issues detected.
     *
     * This is the "happy path" state where:
     * - This saga is not rolling back
     * - No child handlers have failed
     * - Normal execution can proceed
     *
     *   Normal step execution will continue.
     */
    object Gucci : Me.NotRollingBack, Children.NoRollbacks

    /**
     * This saga has successfully rolled back its most recent step.
     *
     * This state indicates:
     * - This saga is actively rolling back
     * - The rollback of the last step completed successfully
     * - No current child rollback issues
     *
     * Rolling back previous steps will continue until all compensating actions are complete.
     *
     * @property throwable The original exception that triggered the rollback
     */
    class SuccessfullyRolledBackLastStep(override val throwable: CooperationException) :
        Me.RollingBack, Children.NoRollbacks, Children.Rollbacks.Successful

    /**
     * Children failed to roll back while this saga was also rolling back.
     *
     * This represents a "double failure" scenario where:
     * - This saga is actively rolling back due to some original failure
     * - Child handlers also failed during their rollback attempts
     *
     * ## Exception Precedence
     *
     * **Important**: Unlike Java's suppressed exceptions, rollback failures take precedence. The
     * child rollback failure becomes the primary exception, with the original rollback cause
     * included as a nested cause.
     *
     * @property throwable [ChildRollbackFailedException] containing both rollback failures and
     *   original cause
     */
    data class ChildrenFailedWhileRollingBackLastStep(
        override val throwable: ChildRollbackFailedException
    ) : Me.RollingBack, Children.Rollbacks.Failed {
        constructor(
            step: String,
            rollbackFailures: List<CooperationException>,
            originalRollbackCause: CooperationException,
        ) : this(
            ChildRollbackFailedException(
                rollbackFailures +
                    listOfNotNull(
                        // Prevent exceptions pointlessly multiplying ad absurdum
                        originalRollbackCause.takeIf { !rollbackFailures.containsRecursively(it) }
                    ),
                step,
            )
        )
    }

    /**
     * Children failed but were successfully rolled back, but this saga was not rolling back yet.
     *
     * This state represents:
     * - Child handlers failed during execution
     * - Child rollbacks completed successfully
     * - This saga is not currently rolling back itself
     *
     * The EventLoop typically handles this by invoking the step's
     * [handleChildFailures][io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep.handleChildFailures]
     * function, which may choose to ignore the failures or trigger this saga's rollback.
     *
     * @property throwable [ChildRolledBackException] containing the original child failures
     */
    class ChildrenFailedAndSuccessfullyRolledBack(
        override val throwable: ChildRolledBackException
    ) : Me.NotRollingBack, Children.Rollbacks.Successful {

        constructor(
            step: String,
            childrenFailures: List<CooperationException>,
        ) : this(ChildRolledBackException(childrenFailures, step))
    }

    /**
     * Children failed and also failed to roll back,but this saga is not rolling back.
     *
     * This state represents:
     * - Child handlers failed during execution
     * - Child rollback attempts also failed
     * - This saga itself is not rolling back
     *
     * The EventLoop typically handles this by invoking the step's
     * [handleChildFailures][io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep.handleChildFailures]
     * function, which may choose to ignore the failures or trigger this saga's rollback.
     *
     * ## Exception Precedence
     *
     * **Important**: Unlike Java's suppressed exceptions, rollback failures take precedence. The
     * rollback failures become the primary exception, with original child failures included as
     * nested causes.
     *
     * @property throwable [ChildRollbackFailedException] containing rollback failures and original
     *   causes
     */
    class ChildrenFailedAndFailedToRollBack(override val throwable: ChildRollbackFailedException) :
        Me.NotRollingBack, Children.Rollbacks.Failed {

        constructor(
            step: String,
            rollbackFailures: List<CooperationException>,
            originalRollbackCauses: List<CooperationException>,
        ) : this(
            ChildRollbackFailedException(
                // This order ensures the first rollback failure is used as the cause
                rollbackFailures + ChildRolledBackException(originalRollbackCauses),
                step,
            )
        )
    }
}

/**
 * Recursively checks if a list of exceptions contains a specific exception.
 *
 * This prevents needless repetitions in exception chaining by detecting when the same exception
 * would be added multiple times to the cause chain.
 *
 * @param exception The exception to search for
 * @return true if the exception is found anywhere in the exception hierarchy
 */
private fun List<CooperationException>.containsRecursively(
    exception: CooperationException
): Boolean = any { it == exception || it.causes.containsRecursively(exception) }

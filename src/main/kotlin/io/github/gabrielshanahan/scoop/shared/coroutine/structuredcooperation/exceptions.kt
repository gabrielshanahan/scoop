package io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.shared.coroutine.ScoopException

/**
 * When a failure downstream causes a rollback of a particular saga run, the exception that is
 * passed to this saga run is the one that originally caused the rollback, wrapped in
 * [ParentSaidSoException]
 *
 * @param cause The original exception from the failed child handler
 */
class ParentSaidSoException(cause: Throwable) : ScoopException(null, cause, false)

/**
 * Exception thrown inside a saga when it is canceled via user request.
 *
 * The constructor removes one frame from the stack trace to point to the actual location where
 * cancel() was called, providing better debugging context.
 *
 * @param reason Human-readable explanation of why the cancellation was requested
 * @see RollbackRequestedException for rollback-specific requests
 */
class CancellationRequestedException(reason: String) : ScoopException(reason, null, true) {
    init {
        // Point to the place where cancel() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

/**
 * Exception used inside a saga when a completed saga needs to be rolled back via user request.
 *
 * This exception represents a request to undo a previously completed saga, typically initiated by
 * business logic or user action. Unlike failure-based rollbacks, this represents a controlled
 * rollback of successfully completed operations.
 *
 * The constructor removes one frame from the stack trace to point to the actual location where
 * rollback() was called, providing better debugging context.
 *
 * @param reason Human-readable explanation of why the rollback was requested
 * @see CancellationRequestedException for cancellation during execution
 */
class RollbackRequestedException(reason: String) : ScoopException(reason, null, true) {
    init {
        // Point to the place where rollback() was actually called
        stackTrace = stackTrace.drop(1).toTypedArray()
    }
}

/**
 * Exception thrown when the EventLoopStrategy determines a saga run should be abandoned.
 *
 * This exception represents a policy decision by the EventLoopStrategy that a saga should no longer
 * be executed, e.g., when deadlines are exceeded. The decision to give up is made by the
 * EventLoopStrategy's `giveUpOnHappyPath` or `giveUpOnRollbackPath` methods.
 *
 * @param causes List of underlying causes that led to the decision to abandon the saga. The first
 *   cause becomes the primary exception, with others as suppressed.
 * @see io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy for
 *   abandonment policy configuration
 */
class GaveUpException(causes: List<Throwable>) : ScoopException(null, causes.first(), false) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

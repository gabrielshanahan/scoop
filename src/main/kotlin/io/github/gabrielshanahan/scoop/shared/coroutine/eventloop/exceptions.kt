package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

import io.github.gabrielshanahan.scoop.shared.coroutine.ScoopException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException

/**
 * Used in situations when one or more child handlers fail during structured cooperation. Multiple
 * child failures are aggregated using the standard Java suppressed exception mechanism. The first
 * failure becomes the primary cause, while subsequent failures are added as suppressed exceptions.
 *
 * @param causes List of CooperationExceptions from failed child handlers. Must not be empty.
 * @param step Optional name of the saga step where the parent was suspended when children failed.
 *   Included in the exception message for debugging context.
 * @see CooperationException for distributed exception structure
 */
class ChildRolledBackException(causes: List<CooperationException>, step: String? = null) :
    ScoopException(
        step?.let { "Child failure occurred while suspended in step [$it]" },
        causes.first(),
        false,
    ) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

/**
 * Used in situations when one or more child rollback handlers fail during distributed rollback.
 * Multiple child failures are aggregated using the standard Java suppressed exception mechanism.
 * The first failure becomes the primary cause, while subsequent failures are added as suppressed
 * exceptions.
 *
 * In Scoop, rollback failures take precedence over original exceptions (unlike Java's suppressed
 * exception model, where exceptions thrown in, e.g., a final block supercede the original
 * exception).
 *
 * @param causes List of Throwables from failed child rollback handlers. Must not be empty.
 * @param step Optional name of the saga step where the parent was suspended when children failed to
 *   roll back. Included in the exception message for debugging context.
 * @see ChildRolledBackException for normal execution failures
 */
class ChildRollbackFailedException(causes: List<Throwable>, step: String? = null) :
    ScoopException(
        step?.let { "Child rollback failure occurred while suspended in step [$it]" },
        causes.first(),
        false,
    ) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

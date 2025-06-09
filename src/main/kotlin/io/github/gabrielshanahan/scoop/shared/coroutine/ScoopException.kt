package io.github.gabrielshanahan.scoop.shared.coroutine

/**
 * ScoopException - Base exception class for Scoop implementation-specific errors
 *
 * This is the parent class for exceptions that are specific to Scoop's implementation of structured
 * cooperation, as opposed to general structured cooperation protocol errors (which are represented
 * by
 * [CooperationFailure][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure]
 * and translated to
 * [CooperationException][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException])
 *
 * ## Distinction from CooperationException
 * - **CooperationException**: Represents distributed failures that propagate across service
 *   boundaries as part of the structured cooperation protocol. These are translations of
 *   language-agnostic
 *   [CooperationFailure][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure]
 *   objects into JVM exceptions.
 * - **ScoopException**: Represents exceptions which are implementation details of Scoop, and are
 *   mostly wrappers that give human-readable names to certain situations. As such, the stack trace
 *   is often disabled.
 *
 * @param message Human-readable error description
 * @param cause The underlying exception that triggered this error
 * @param stackTrace Whether to capture stack trace (performance optimization)
 * @see io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
 *   for distributed exception propagation
 * @see io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
 *   for language-agnostic failure representation
 */
abstract class ScoopException(message: String?, cause: Throwable?, stackTrace: Boolean) :
    Exception(message, cause, true, stackTrace)

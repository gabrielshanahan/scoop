package io.github.gabrielshanahan.scoop.coroutine

import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ReturnValueAlreadyExistsException

/**
 * Thrown when one of Scoop's OWN persistence operations fails — a query Scoop runs for its own
 * bookkeeping (writing `message_event` rows, reading the give-up set, inserting emitted messages,
 * storing/reading return values, …) — as opposed to anything the saga's own step code does.
 *
 * The overwhelmingly common cause is a dead/closed JDBC connection (e.g. one that rotted while idle
 * and was then handed out of the pool). The defining property is that the failure originates
 * **outside the code that defines the saga**, so it carries no information about whether the saga's
 * business action should be compensated — and must therefore NEVER drive a rollback. The event loop
 * treats it as a transient tick failure: the current tick's transaction is rolled back and the saga
 * is re-resumed (retried) on a later tick from its last committed step, rather than unwinding the
 * whole saga. This is what keeps a perpetual (infinite-`GoTo`) saga alive across a connection blip.
 *
 * Stack trace is suppressed (like the other [ScoopException]s) — the wrapped [cause] carries it.
 *
 * @param cause The underlying failure from Scoop's persistence layer (typically a SQL/connection
 *   exception).
 */
class ScoopInfrastructureException(cause: Throwable) :
    ScoopException(
        "A Scoop bookkeeping operation failed (most often a dead JDBC connection). This is not a " +
            "saga-logic failure and must not trigger rollback.",
        cause,
        false,
    )

/**
 * Runs [block] — a Scoop-internal persistence operation — re-classifying any failure that is NOT a
 * deliberate logical signal as a [ScoopInfrastructureException], so the event loop retries the tick
 * instead of rolling the saga back. See [ScoopInfrastructureException] for the rationale.
 *
 * Pass-throughs (NOT wrapped, because they are meaningful outcomes rather than infrastructure
 * faults):
 * - any [ScoopException] (which includes [ScoopInfrastructureException] itself, plus the deliberate
 *   control signals such as gave-up / cancellation / rollback-requested), and
 * - [ReturnValueAlreadyExistsException] (a logical uniqueness outcome that callers branch on).
 *
 * [Error]s (OOM, StackOverflow, …) are deliberately NOT caught: an `Error` is not Scoop's verdict
 * on the saga either, and must propagate untouched to fail the tick (or crash the process), never
 * be swallowed into the retry path.
 */
// TooGenericExceptionCaught is the whole point: any non-logical failure of Scoop's own persistence
// is reclassified as infrastructure, regardless of its concrete type. Errors are not caught.
@Suppress("TooGenericExceptionCaught")
internal inline fun <T> asScoopInfrastructure(block: () -> T): T =
    try {
        block()
    } catch (e: ScoopException) {
        throw e
    } catch (e: ReturnValueAlreadyExistsException) {
        throw e
    } catch (e: Exception) {
        throw ScoopInfrastructureException(e)
    }

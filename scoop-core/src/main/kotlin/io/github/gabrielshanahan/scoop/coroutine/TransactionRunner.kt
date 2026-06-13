package io.github.gabrielshanahan.scoop.coroutine

import io.github.gabrielshanahan.scoop.transactional
import java.sql.Connection
import org.codejargon.fluentjdbc.api.FluentJdbc

/**
 * Owns the database transaction that wraps a single saga step.
 *
 * Each tick of the [EventLoop] resumes one saga step inside a transaction. Scoop's own
 * `message_event` writes happen on the [Connection] handed to [inStepTransaction], and so does any
 * business code the step calls. Keeping those writes in one transaction is what makes a step
 * atomic: either the business effect and the structured-cooperation bookkeeping both commit, or
 * both roll back.
 *
 * This seam exists so integrations can decide *how* that per-step transaction is managed. The
 * framework-agnostic default ([FluentJdbcTransactionRunner]) opens and manages its own plain JDBC
 * transaction. A JTA-based integration (e.g. Quarkus/Narayana) can instead begin a container
 * transaction and hand back the transaction-enlisted connection, so that business code annotated
 * with `@Transactional` joins the very same transaction on the very same connection as scoop's
 * writes.
 *
 * Only the per-step transaction is routed through this seam. The other transactions in the event
 * loop (starting continuations, and the separate-thread rollback markers) run no business code and
 * deliberately remain plain self-managed transactions.
 */
interface TransactionRunner {
    /**
     * Runs [block] inside the per-step transaction, passing it the transaction's [Connection].
     *
     * The transaction is committed if [block] returns normally and rolled back if it throws.
     *
     * @param block the step work to run; receives the transaction-bound connection
     * @return whatever [block] returns
     */
    fun <T> inStepTransaction(block: (Connection) -> T): T
}

/**
 * Default [TransactionRunner] that manages its own plain JDBC transaction via [FluentJdbc].
 *
 * Behaves exactly like calling [fluentJdbc].[transactional][transactional] directly: autocommit
 * off, commit on normal return, rollback on throw. This preserves Scoop's plain-JVM behavior
 * unchanged.
 */
class FluentJdbcTransactionRunner(private val fluentJdbc: FluentJdbc) : TransactionRunner {
    override fun <T> inStepTransaction(block: (Connection) -> T): T =
        fluentJdbc.transactional(block)
}

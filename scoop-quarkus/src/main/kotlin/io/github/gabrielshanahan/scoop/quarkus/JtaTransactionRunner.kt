package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.coroutine.TransactionRunner
import io.quarkus.narayana.jta.QuarkusTransaction
import java.sql.Connection
import javax.sql.DataSource

/**
 * [TransactionRunner] that runs each saga step inside a fresh JTA (Narayana) transaction.
 *
 * [QuarkusTransaction.requiringNew] begins a new container transaction; the [Connection] obtained
 * from the Agroal [dataSource] inside that transaction is automatically enlisted in it. Because
 * Agroal returns the same transaction-enlisted connection for every acquisition within a single JTA
 * transaction, any business code annotated with `@Transactional(REQUIRED)` that the step calls
 * joins this exact transaction on this exact connection. Scoop's `message_event` writes therefore
 * become atomic with the business writes: the transaction commits on normal return and rolls back
 * on throw.
 */
class JtaTransactionRunner(private val dataSource: DataSource) : TransactionRunner {
    override fun <T> inStepTransaction(block: (Connection) -> T): T =
        QuarkusTransaction.requiringNew().call { dataSource.connection.use { conn -> block(conn) } }
}

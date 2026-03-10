package io.github.gabrielshanahan.scoop

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.groups.MultiTimePeriod
import java.sql.Connection
import java.time.Duration
import org.codejargon.fluentjdbc.api.FluentJdbc

/**
 * Executes a block of code within a database transaction using FluentJdbc.
 *
 * This extension function provides a convenient way to execute database operations within a
 * transactional context. The transaction is automatically committed if the block completes
 * successfully, or rolled back if an exception is thrown. This is essential for maintaining data
 * consistency in saga step execution.
 *
 * ## Structured Cooperation Role
 * Used throughout Scoop to ensure saga state changes are atomic. Each saga step execution typically
 * runs within a transaction to ensure message events and business data are persisted consistently.
 *
 * ## Usage Patterns
 *
 * ```kotlin
 * fluentJdbc.transactional { connection ->
 *     messageEventRepository.persistStepCompletion(connection, event)
 *     businessRepository.updateRecord(connection, data)
 * }
 * ```
 *
 * @param block Code to execute within the transaction, receives the database connection
 * @return Result of the block execution
 * @throws Exception Any exception thrown by the block will cause transaction rollback
 */
inline fun <T> FluentJdbc.transactional(crossinline block: (Connection) -> T): T =
    query().let { query ->
        query.transaction().`in` { query.plainConnection { connection -> block(connection) } }
    }

/**
 * Control flow utility for conditional iteration with explicit continuation signaling.
 *
 * This function provides a pattern for loops where the decision to continue must be made explicitly
 * within the loop body. Unlike traditional while loops, continuation requires an explicit call to
 * the saySo callback. This pattern is useful for event loop implementations where processing should
 * continue only under specific conditions.
 *
 * ## Usage Patterns
 * Commonly used in event loop tick processing where repetition depends on whether work was found
 * and processed:
 * ```kotlin
 * whileISaySo { repeatCount, saySo ->
 *     val workFound = processNextBatch()
 *     if (workFound) {
 *         saySo() // Continue processing
 *     }
 *     // If saySo() not called, loop exits
 * }
 * ```
 *
 * ## Integration Points
 * Used by EventLoop implementations to control processing cycles, ensuring the loop only continues
 * when there's actual work to be done rather than busy-waiting.
 *
 * @param block Code to execute in each iteration. Receives current repeat count and a callback
 *   function to signal continuation
 */
inline fun whileISaySo(crossinline block: (repeatCount: Int, saySo: () -> Unit) -> Unit) {
    var repeatCount = 0
    var repeat = true
    while (repeat) {
        repeat = false
        repeatCount++
        block(repeatCount) { repeat = true }
    }
}

/**
 * Creates a Multi that emits ticks at approximately the given duration, with jitter applied.
 *
 * This extension function adds a small random delay (2% of the interval) to each tick to avoid
 * [thundering herd problems](https://en.wikipedia.org/wiki/Thundering_herd_problem) when multiple
 * instances are running periodic tasks.
 *
 * @param duration The base interval between ticks
 * @return A Multi that emits Long values at jittered intervals
 */
fun MultiTimePeriod.everyJittered(duration: Duration): Multi<Long> =
    every(duration)
        .onOverflow()
        .drop() // avoid backpressure issues with delay
        .onItem()
        .transformToUniAndConcatenate { tick ->
            val jitterMillis = duration.toMillis() * 0.02
            Uni.createFrom()
                .item(tick)
                .onItem()
                .delayIt()
                .by(Duration.ofMillis(jitterMillis.toLong()))
        }

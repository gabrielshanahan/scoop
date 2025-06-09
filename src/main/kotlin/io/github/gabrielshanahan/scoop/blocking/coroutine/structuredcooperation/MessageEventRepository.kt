package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.isNotEmpty
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.intellij.lang.annotations.Language
import org.postgresql.util.PGobject

/**
 * Handles all database operations for the `message_event` table, which tracks saga execution state.
 * 
 * The `message_event` table is the heart of structured cooperation's implementation. It tracks
 * the lifecycle of every saga execution, from initial message emission through completion or rollback.
 * This repository provides all the SQL operations needed to maintain this state.
 * 
 * ## Core Responsibilities
 * 
 * - **Event Recording**: Insert various types of message events (EMITTED, SEEN, SUSPENDED, etc.)
 * - **State Querying**: Find pending saga runs that are ready to be resumed  
 * - **Concurrency Control**: Use database locking to ensure safe concurrent saga execution
 * - **Rollback Coordination**: Handle complex rollback event insertion and tracking
 * 
 * ## Event Types
 * 
 * The repository handles these key event types:
 * - `EMITTED`: A message was sent (creates cooperation lineage)
 * - `SEEN`: A handler started processing a message (with cooperation lineage extension)
 * - `SUSPENDED`: A step completed and is waiting for child handlers
 * - `COMMITTED`: A saga completed successfully
 * - `ROLLING_BACK`: A saga entered rollback mode due to failure
 * - `ROLLBACK_EMITTED`: Rollback messages were sent to child handlers
 * - `ROLLED_BACK`: A saga completed its rollback successfully
 * - `ROLLBACK_FAILED`: A saga's rollback compensating action threw an exception
 * - `CANCELLATION_REQUESTED`: External cancellation was requested
 * 
 * ## Cooperation Lineage
 * 
 * The cooperation lineage (UUID array) is the key mechanism that enables structured cooperation.
 * It tracks parent-child relationships between saga executions:
 * - Parent messages have shorter lineages
 * - Child messages extend the parent's lineage with a new UUID
 * - This creates a tree structure that enables dependency tracking
 * 
 * ## Concurrency and Locking
 * 
 * The repository uses sophisticated PostgreSQL locking patterns to ensure safe concurrent
 * execution of multiple saga instances. See [startContinuationsForCoroutine] for details
 * on the locking strategy used to prevent race conditions.
 * 
 * For the theoretical foundation, see: https://developer.porn/posts/implementing-structured-cooperation/
 * For cooperation lineage details, see: https://developer.porn/posts/introducing-structured-cooperation/
 */
@ApplicationScoped
class MessageEventRepository(
    private val jsonbHelper: JsonbHelper,
    private val fluentJdbc: FluentJdbc,
) {

    /**
     * Records that a message was emitted on the global scope (breaking cooperation lineage).
     * 
     * Global scope emissions create new cooperation roots rather than extending existing lineages.
     * This is used when sagas deliberately want to break structured cooperation for decoupling.
     * 
     * @param connection Database connection for the transaction
     * @param messageId ID of the emitted message
     * @param cooperationLineage New cooperation lineage starting from this emission
     * @param context Optional cooperation context to propagate to independent handlers
     */
    fun insertGlobalEmittedEvent(
        connection: Connection,
        messageId: UUID,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                "--${messageId} || ${cooperationLineage}\n" +
                    "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES (:messageId, 'EMITTED', :cooperationLineage, :context)"
            )
            .namedParam("messageId", messageId)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    /**
     * Records that a message was emitted within a cooperation scope (extending the lineage).
     * 
     * Scoped emissions are the normal case in structured cooperation - they create child
     * cooperation scopes that extend the parent's lineage. The emitting saga will suspend
     * after this step until all handlers of this message complete.
     * 
     * @param connection Database connection for the transaction
     * @param messageId ID of the emitted message
     * @param coroutineName Name of the saga that emitted the message
     * @param coroutineIdentifier Unique identifier for the saga instance
     * @param stepName Name of the step that emitted the message
     * @param cooperationLineage Extended cooperation lineage for child handlers
     * @param context Optional cooperation context to propagate to child handlers
     */
    fun insertScopedEmittedEvent(
        connection: Connection,
        messageId: UUID,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                """
                INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) 
                VALUES (:messageId, 'EMITTED', :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :context)
            """
                    .trimIndent()
            )
            .namedParam("messageId", messageId)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun insertRollbackEmittedEvent(
        connection: Connection,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH message_id_lookup AS (
                    SELECT DISTINCT message_id 
                    FROM message_event 
                    WHERE cooperation_lineage = :cooperationLineage
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id, 
                    type,  
                    cooperation_lineage, 
                    exception
                )
                SELECT 
                    message_id_lookup.message_id, 'ROLLBACK_EMITTED', :cooperationLineage, :exception
                FROM message_id_lookup
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM message_event seen
                    LEFT JOIN message_event committed
                      ON committed.cooperation_lineage = seen.cooperation_lineage
                         AND committed.type = 'COMMITTED'
                    WHERE seen.type = 'SEEN'
                      AND seen.cooperation_lineage <@ :cooperationLineage
                      AND committed.cooperation_lineage IS NULL
                      AND cardinality(seen.cooperation_lineage) > 1
                )
                ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .run()
    }

    fun insertCancellationRequestedEvent(
        connection: Connection,
        coroutineName: String?,
        coroutineIdentifier: String?,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH message_id_lookup AS (
                    SELECT DISTINCT message_id 
                    FROM message_event 
                    WHERE cooperation_lineage = :cooperationLineage
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id, 
                    type, 
                    coroutine_name, 
                    coroutine_identifier, 
                    step, 
                    cooperation_lineage, 
                    exception
                ) 
                SELECT 
                    message_id_lookup.message_id,
                    'CANCELLATION_REQUESTED',
                    :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :exception
                FROM message_id_lookup
                ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .run()
    }

    fun insertMessageEvent(
        connection: Connection,
        messageId: UUID,
        messageEventType: String,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure?,
        context: CooperationContext?,
    ) {
        fluentJdbc
            .queryOn(connection)
            .update(
                """
                INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) 
                VALUES (:messageId, :messageEventType::message_event_type, :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage, :exception, :context)
            """
                    .trimIndent()
            )
            .namedParam("messageId", messageId)
            .namedParam("messageEventType", messageEventType)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("stepName", stepName)
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .namedParam("exception", exception?.let { jsonbHelper.toPGobject(it) })
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun insertRollbackEmittedEventsForStep(
        connection: Connection,
        stepName: String,
        cooperationLineage: List<UUID>,
        coroutineName: String,
        coroutineIdentifier: String,
        scopeStepName: String?,
        exception: CooperationFailure,
        context: CooperationContext?,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        fluentJdbc
            .queryOn(connection)
            .update(
                """
                WITH emitted_record AS (
                    SELECT cooperation_lineage, message_id
                    FROM message_event
                    WHERE type = 'EMITTED'
                        AND step = :stepName
                        AND cooperation_lineage = :cooperationLineage
                )
                INSERT INTO message_event (
                    message_id, type, 
                    cooperation_lineage, coroutine_name, coroutine_identifier, step,
                    exception, context
                )
                SELECT 
                    message_id, 'ROLLBACK_EMITTED', 
                    :cooperationLineage, :coroutineName, :coroutineIdentifier, :scopeStepName, :exception, :context
                FROM emitted_record
            """
                    .trimIndent()
            )
            .namedParam("stepName", stepName)
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("coroutineName", coroutineName)
            .namedParam("coroutineIdentifier", coroutineIdentifier)
            .namedParam("scopeStepName", scopeStepName)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .namedParam(
                "context",
                context?.takeIf { it.isNotEmpty() }?.let { jsonbHelper.toPGobject(it) },
            )
            .run()
    }

    fun fetchGiveUpExceptions(
        connection: Connection,
        giveUpSqlProvider: (String) -> String,
        cooperationLineage: List<UUID>,
    ): List<CooperationException> =
        fluentJdbc
            .queryOn(connection)
            .select(
                """
                WITH seen AS (
                    SELECT * FROM message_event WHERE cooperation_lineage = :cooperationLineage AND type = 'SEEN'
                )
                ${giveUpSqlProvider("seen")}
            """
                    .trimIndent()
            )
            .namedParam(
                "cooperationLineage",
                connection.createArrayOf("uuid", cooperationLineage.toTypedArray()),
            )
            .listResult { resultSet ->
                val cooperationFailure =
                    jsonbHelper.fromPGobject<CooperationFailure>(resultSet.getObject("exception"))
                cooperationFailure?.let { CooperationFailure.Companion.toCooperationException(it) }
            }
            .filterNotNull()

    /**
     * Starts new continuations for a given coroutine by creating SEEN and ROLLING_BACK events.
     * 
     * This is the entry point for saga execution - it finds EMITTED events that don't have
     * corresponding SEEN events and creates them, extending the cooperation lineage. It also
     * handles ROLLBACK_EMITTED events that need corresponding ROLLING_BACK events.
     * 
     * ## Concurrency Control
     * 
     * This method uses sophisticated PostgreSQL locking to ensure safe concurrent execution:
     * - Uses FOR UPDATE SKIP LOCKED to prevent multiple instances from processing the same message
     * - Validates that parent events are in the correct state before proceeding
     * - Checks that the EventLoopStrategy's start conditions are met
     * 
     * ## Race Condition Prevention
     * 
     * The locking strategy prevents a critical race condition: if a parent coroutine is picked up
     * due to a timeout condition becoming true, we need to guarantee that all child SEEN events
     * have actually terminated. Without proper locking, a child could register a SEEN event at 
     * the precise moment the parent starts cancellation, leading to inconsistent state.
     * 
     * ## Rollback Support
     * 
     * For rollbacks, we don't do the last_parent_event check to support partial rollbacks of
     * only some subtree (as demonstrated in the "rolling back sub-hierarchy works" test).
     * 
     * @param connection Database connection for the transaction
     * @param coroutineName Name of the coroutine to start continuations for
     * @param coroutineIdentifier Unique identifier for coroutine instances
     * @param topic Message topic this coroutine handles
     * @param eventLoopStrategy Strategy that determines when continuations should start
     */
    fun startContinuationsForCoroutine(
        connection: Connection,
        coroutineName: String,
        coroutineIdentifier: String,
        topic: String,
        eventLoopStrategy: EventLoopStrategy,
    ) {
        // The lock gymnastics are necessary in combination with the strategy evaluation in
        // PendingCoroutineRunSql - specifically, we need to guarantee that the "All seens have
        // been terminated" holds when a parent coroutine is picked up due to a giving up condition
        // becoming true. The quintessential place where this is necessary is a network partition,
        // where a certain coroutine expected by the strategy is unavailable for the entire time 
        // until a timeout happens (in which case we correctly don't want to block the revert),
        // but then, at the precise moment we pick up the parent to start the cancellation, it
        // appears, registers a SEEN, and starts executing.

        // We don't do the last_parent_event check for rollbacks to support partial rollbacks of 
        // only some subtree, as in test `rolling back sub-hierarchy works`

        // TODO: Move this to some repository
        @Language("PostgreSQL")
        val sql =
            """
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT emitted.message_id, emitted.cooperation_lineage, emitted.context, emitted.step
                    FROM message_event emitted
                    LEFT JOIN message_event AS coroutine_seen
                        ON coroutine_seen.message_id = emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = :coroutine_name
                    JOIN message
                        ON message.id = emitted.message_id AND message.topic = :topic
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = emitted.cooperation_lineage
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    LEFT JOIN LATERAL (
                        -- Get the last event in parent sequence along with its type and step
                        SELECT type, step
                        FROM message_event last_event
                        WHERE last_event.cooperation_lineage = parent_seen_exists.cooperation_lineage
                        ORDER BY last_event.created_at DESC
                        LIMIT 1
                    ) last_parent_event ON parent_seen_exists.id IS NOT NULL
                    WHERE emitted.type = 'EMITTED' 
                        AND coroutine_seen.id IS NULL
                        AND (
                            -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                            parent_seen_exists.id IS NULL
                            -- OR the SEEN record exists AND we successfully locked it AND the parent sequence's last event is SUSPENDED with matching step
                            OR (
                                parent_seen_exists.id IS NOT NULL 
                                AND parent_seen_lock_attempt.locked IS NOT NULL
                                AND last_parent_event.type = 'SUSPENDED'
                                AND last_parent_event.step = emitted.step
                            )
                        )
                        AND (${eventLoopStrategy.start("emitted")})
                ),
                -- Find ROLLBACK_EMITTED records missing a ROLLING_BACK
                rollback_emitted_missing_rolling_back AS (
                    SELECT rollback_emitted.message_id, coroutine_seen.cooperation_lineage, rollback_emitted.exception, rollback_emitted.context, rollback_emitted.step
                    FROM message_event rollback_emitted
                    LEFT JOIN message_event AS rolling_back_check
                        ON rolling_back_check.message_id = rollback_emitted.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = :coroutine_name
                    JOIN message
                        ON message.id = rollback_emitted.message_id AND message.topic = :topic
                    JOIN message_event AS coroutine_seen
                         ON coroutine_seen.message_id = rollback_emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = :coroutine_name
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = rollback_emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = rollback_emitted.cooperation_lineage 
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    WHERE rollback_emitted.type = 'ROLLBACK_EMITTED' 
                        AND rolling_back_check.id IS NULL
                        AND (
                                -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                                parent_seen_exists.id IS NULL
                                -- OR the SEEN record exists AND we successfully locked it AND the parent sequence's last event is SUSPENDED with matching step
                                OR (
                                    parent_seen_exists.id IS NOT NULL 
                                    AND parent_seen_lock_attempt.locked IS NOT NULL
                                )
                            )
                ),
                -- Insert SEEN if EMITTED exists without SEEN
                seen_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage,
                        context
                    )
                    SELECT 
                        emitted_missing_seen.message_id, 
                        'SEEN', 
                        :coroutine_name,
                        :coroutine_identifier,
                        emitted_missing_seen.cooperation_lineage || gen_uuid_v7(), -- append additional cooperation id
                        emitted_missing_seen.context
                    FROM emitted_missing_seen
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'SEEN' DO NOTHING
                    RETURNING id
                ),
                -- Insert ROLLING_BACK if ROLLBACK_EMITTED exists without ROLLING_BACK
                rolling_back_insert AS (
                    INSERT INTO message_event (
                        message_id, type, 
                        coroutine_name, coroutine_identifier, 
                        cooperation_lineage, 
                        exception,
                        context
                    )
                    SELECT 
                        rollback_emitted_missing_rolling_back.message_id, 
                        'ROLLING_BACK', 
                        :coroutine_name,
                        :coroutine_identifier,
                        rollback_emitted_missing_rolling_back.cooperation_lineage,
                        rollback_emitted_missing_rolling_back.exception,
                        rollback_emitted_missing_rolling_back.context
                    FROM rollback_emitted_missing_rolling_back
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'ROLLING_BACK' DO NOTHING
                    RETURNING id
                )
                -- Just here to execute the CTEs
                SELECT 1;
            """
                .trimIndent()

        fluentJdbc
            .queryOn(connection)
            .select(sql)
            .namedParam("coroutine_name", coroutineName)
            .namedParam("coroutine_identifier", coroutineIdentifier)
            .namedParam("topic", topic)
            .singleResult {}
    }

    /**
     * Represents a saga execution that is ready to be resumed.
     * 
     * This data class contains all the information needed to build a [Continuation] and
     * resume saga execution from where it left off. It's the result of the complex SQL
     * queries in [PendingCoroutineRunSql] that determine which sagas are ready to proceed.
     * 
     * @param messageId The ID of the message being processed
     * @param topic The message topic
     * @param cooperationLineage The cooperation lineage for this saga execution
     * @param payload The message payload
     * @param createdAt When the message was originally created
     * @param step The last step that was suspended (null if not suspended yet)
     * @param latestScopeContext The latest scope-level context (shared across cooperation lineage)
     * @param latestContext The latest step-level context (local to this saga)
     * @param childRolledBackExceptions JSON array of exceptions from child handlers that rolled back successfully
     * @param childRollbackFailedExceptions JSON array of exceptions from child rollbacks that failed
     * @param rollingBackException The exception that caused this saga to enter rollback mode (if any)
     */
    data class PendingCoroutineRun(
        val messageId: UUID,
        val topic: String,
        val cooperationLineage: List<UUID>,
        val payload: PGobject,
        val createdAt: OffsetDateTime,
        val step: String?,
        val latestScopeContext: PGobject?,
        val latestContext: PGobject?,
        val childRolledBackExceptions: PGobject,
        val childRollbackFailedExceptions: PGobject,
        val rollingBackException: PGobject?,
    )

    /**
     * Finds and locks a saga execution that is ready to be resumed.
     * 
     * This method implements the core structured cooperation logic by querying for sagas
     * that are ready to proceed based on the [EventLoopStrategy]. It uses the complex
     * SQL queries defined in [PendingCoroutineRunSql] to determine readiness.
     * 
     * ## Double-Checked Locking Pattern
     * 
     * This method implements a variant of double-checked locking to prevent race conditions:
     * 1. First query: Find a saga that appears ready and get its ID
     * 2. Lock the saga: Use FOR UPDATE SKIP LOCKED to acquire an exclusive lock
     * 3. Second query: Re-evaluate readiness with the lock held
     * 
     * This pattern is necessary because PostgreSQL's FOR UPDATE SKIP LOCKED uses the state
     * of locks at execution time, not at the beginning of the transaction. Without the
     * double-check, we could pick up a saga with stale data if another handler committed
     * changes between our readiness evaluation and lock acquisition.
     * 
     * ## Structured Cooperation Logic
     * 
     * The readiness determination follows structured cooperation rules:
     * - For happy path: All child handlers must have completed (COMMITTED/ROLLED_BACK/ROLLBACK_FAILED)
     * - For rollback path: All child rollbacks must have completed
     * - EventLoopStrategy can override these rules (e.g., for timeouts, cancellation)
     * 
     * @param connection Database connection for the transaction
     * @param coroutineName Name of the coroutine to fetch pending runs for
     * @param eventLoopStrategy Strategy that determines readiness conditions
     * @return A pending coroutine run ready for execution, or null if none are ready
     */
    fun fetchPendingCoroutineRun(
        connection: Connection,
        coroutineName: String,
        eventLoopStrategy: EventLoopStrategy,
    ): PendingCoroutineRun? {
        val firstResult =
            fluentJdbc
                .queryOn(connection)
                .select("--$coroutineName\n" + finalSelect(eventLoopStrategy).build())
                .namedParam("coroutine_name", coroutineName)
                .firstResult { resultSet -> resultSet.getObject("id") as UUID }
                .orElse(null)

        if (firstResult == null) {
            // Nothing to do -> we're done
            return null
        } else {
            // This is, in essence, the equivalent of double-checked locking
            // (https://en.wikipedia.org/wiki/Double-checked_locking).
            //
            // The query in finalSelect() essentially works in two steps:
            //    1. Evaluate which SEEN records are waiting to be processed
            //    2. Pick one and lock it, skipping locked ones.
            //
            // An important refresher: FOR UPDATE SKIP LOCKED uses the state of locks
            // at the time it's executed, not at the beginning of the transaction.
            //
            // A race condition can arise in the following way:
            //
            // During the evaluation of step 1, some other handler can already be
            // processing a SEEN, but not have committed yet, so we still get "ready to be
            // processed" for that SEEN. But before we get to step 2, where the lock would
            // prevent us from picking it up, the handler commits and releases the lock.
            // This causes us to pick up the SEEN, but still retain and use the old data
            // we fetched earlier, resulting in us re-running the same step again.
            // Therefore,
            // we mitigate this by rerunning the selection process once we have acquired the
            // lock.
            //
            // Postgres attempts to mitigate this to a certain extent by checking that the
            // row being locked has not been modified during the time the query is run and
            // refetches the record if that happens. Some solutions to this problem revolve
            // around this, recommending you always modify the row being locked. However,
            // Postgres only refetches that specific row and doesn't evaluate the rest of
            // the conditions. This makes it unusable for our purposes, since, e.g., the
            // latest SUSPEND record is one of the key things we use and need to refetch.
            //
            // There are various other approaches that could be taken, sometimes referred
            // to as "materializing the conflict", but the simplest solution for a POC
            // application such as this is to just run the query again after we've
            // acquired the lock, which is exactly what double-checked locking is.
            //
            // For a different example of this behavior, take a look at, e.g.,
            // https://postgrespro.com/list/thread-id/2470837
            return fluentJdbc
                .queryOn(connection)
                .select(finalSelect(eventLoopStrategy, true).build())
                .namedParam("coroutine_name", coroutineName)
                .namedParam("message_id", firstResult)
                .firstResult {
                    PendingCoroutineRun(
                        messageId = it.getObject("id") as UUID,
                        topic = it.getString("topic"),
                        payload = it.getObject("payload") as PGobject,
                        createdAt =
                            it.getTimestamp("created_at")
                                .toLocalDateTime()
                                .atOffset(ZoneOffset.UTC),
                        cooperationLineage =
                            (it.getArray("cooperation_lineage").array as Array<UUID>).toList(),
                        step = it.getString("step"),
                        latestScopeContext = it.getObject("latest_scope_context") as PGobject?,
                        latestContext = it.getObject("latest_context") as PGobject?,
                        childRolledBackExceptions =
                            it.getObject("child_rolled_back_exceptions") as PGobject,
                        childRollbackFailedExceptions =
                            it.getObject("child_rollback_failed_exceptions") as PGobject,
                        rollingBackException = it.getObject("rolling_back_exception") as PGobject?,
                    )
                }
                // After the second fetch, we found out that the record is no longer ready for
                // processing (i.e., the race condition described above happened)
                .orElse(null)
        }
    }
}

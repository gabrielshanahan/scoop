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
     * cooperation scopes that extend the parent's lineage. The emitting saga will suspend after
     * this step until all handlers of this message complete.
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

    /**
     * Records that a rollback request has been emitted for a specific cooperation lineage.
     *
     * This method creates a `ROLLBACK_EMITTED` event to signal that a saga and all its children
     * should enter rollback mode. This will get picked up by the logic in
     * [startContinuationsForCoroutine], causing the rollback to start.
     *
     * The method only emits rollback events when it's safe and sensible to do so - specifically,
     * when there are no child handlers that are still actively running.
     *
     * ## SQL Logic Breakdown
     *
     * The complex WHERE clause implements a critical safety check:
     * 1. **Find all descendant SEEN events**: Looks for child cooperation lineages using `<@`
     *    operator
     * 2. **Check completion status**: Verifies each child has a corresponding COMMITTED or
     *    ROLLBACK_FAILED event
     * 3. **Block rollback if active children exist**: Only proceeds if no children are still
     *    running
     *
     * Note: We check for both COMMITTED and ROLLBACK_FAILED because if something rolled back
     * successfully, then everything must've rolled back, and there's no point in emitting a
     * rollback here. We only need to wait for children that are still actively executing to reach
     * completion.
     *
     * @param connection Database connection for the transaction
     * @param cooperationLineage The cooperation lineage to initiate rollback for
     * @param exception The failure that triggered the rollback request
     */
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
                    -- Find the message_id associated with this cooperation lineage
                    -- Any event from the same lineage will have the same message_id
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
                    -- Safety check: Only emit rollback if no actively running children exist
                    -- Checking for COMMITTED and ROLLBACK_FAILED is enough because if something
                    -- rolled back, then everything must've rolled back, and there's no point in
                    -- emitting anything here
                    SELECT 1
                    FROM message_event seen
                    LEFT JOIN message_event terminated
                      ON terminated.cooperation_lineage = seen.cooperation_lineage
                         AND terminated.type in ('COMMITTED', 'ROLLBACK_FAILED')
                    WHERE seen.type = 'SEEN'                                    -- Child handler started
                      AND seen.cooperation_lineage <@ :cooperationLineage       -- Is descendant of our lineage
                      AND terminated.cooperation_lineage IS NULL                -- But hasn't successfully completed yet
                )
                -- Prevent duplicate rollback events for the same message
                ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
            .namedParam("exception", jsonbHelper.toPGobject(exception))
            .run()
    }

    /**
     * Records a cancellation request for a specific cooperation lineage.
     *
     * This method creates a `CANCELLATION_REQUESTED` event that signals a saga and its entire
     * cooperation hierarchy should be cancelled. Cancellation requests are detected by
     * [EventLoopStrategy] implementations and cause sagas to fail with a
     * [CancellationRequestedException][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException].
     *
     * ## Cancellation Implementation Overview
     *
     * Cancellation in structured cooperation follows these key components:
     *
     * ### Initiation
     * - **[Capabilities.cancel]**: High-level API for initiating cancellation requests
     * - **[CancellationRequestedException][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException]**:
     *   Exception type thrown when cancellation is detected
     *
     * ### Detection
     * - **[cancellationRequested()][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.cancellationRequested]**
     *   (strategyBuilders.kt): SQL generator for detecting cancellation events
     * - **[EventLoopStrategy]**: Custom strategies can use `cancellationRequested()` in give-up
     *   conditions
     *
     * ### Execution
     * - **[fetchGiveUpExceptions]**: Retrieves cancellation exceptions for throwing
     * - **[CooperationScope.giveUpIfNecessary][io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope.giveUpIfNecessary]**:
     *   Checks for and throws cancellation exceptions during saga execution
     *
     * @param connection Database connection for the transaction
     * @param cooperationLineage The cooperation lineage to cancel
     * @param exception The cancellation details (reason, source, etc.)
     * @see
     *   io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException
     * @see Capabilities.cancel
     * @see fetchGiveUpExceptions
     * @see
     *   io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.cancellationRequested
     */
    fun insertCancellationRequestedEvent(
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
                    coroutine_name, coroutine_identifier, step, 
                    cooperation_lineage, 
                    exception
                ) 
                SELECT 
                    message_id_lookup.message_id,
                    'CANCELLATION_REQUESTED',
                    null, null, null, 
                    :cooperationLineage, 
                    :exception
                FROM message_id_lookup
                ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
            """
                    .trimIndent()
            )
            .namedParam("cooperationLineage", lineageArray)
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

    /**
     * Records rollback requests for all messages emitted by a specific step.
     *
     * This method creates `ROLLBACK_EMITTED` events for every message that was emitted during a
     * specific step's execution. It's a key component of structured cooperation's rollback
     * mechanism, ensuring that child handlers are instructed to roll back when their parent step
     * needs to roll back.
     *
     * This method is called during rollback continuation execution via
     * [ScopeCapabilities.emitRollbacksForEmissions].
     */
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

    /**
     * Used to implement
     * [CooperationScope.giveUpIfNecessary][io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope.giveUpIfNecessary],
     * via [ScopeCapabilities.giveUpIfNecessary].
     *
     * The [giveUpSqlProvider] accepts as input the alias of the CTE containing the SEEN event of
     * the scope, and is expected to return a valid SELECT query that provides (one or more) JSONs
     * of [CooperationFailure] instances under the column `exception`. See
     * [MessageEventRepository.fetchGiveUpExceptions] for details.
     *
     * The actual implementation of [giveUpSqlProvider] delegates to either
     * [EventLoopStrategy.giveUpOnHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnHappyPath]
     * or
     * [EventLoopStrategy.giveUpOnRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.giveUpOnRollbackPath].
     */
    fun fetchGiveUpExceptions(
        connection: Connection,
        giveUpSqlProvider: (seenAlias: String) -> String,
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
                cooperationFailure.let { CooperationFailure.Companion.toCooperationException(it) }
            }
            .filterNotNull()

    /**
     * Creates SEEN and ROLLING_BACK events for a given coroutine, thereby effectively starting new
     * continuations.
     *
     * This is the entry point for saga execution - it finds EMITTED events that don't have
     * corresponding SEEN events and creates them, extending the cooperation lineage. It also
     * handles ROLLBACK_EMITTED events that need corresponding ROLLING_BACK events.
     *
     * This method uses Postgres locking to ensure safe concurrent execution:
     * - Uses FOR UPDATE SKIP LOCKED to prevent race conditions from happening when multiple
     *   instances of the saga are running
     * - Validates that parent events are in the correct state before proceeding (also needed to
     *   prevent the race condition)
     * - Checks that the EventLoopStrategy's [start][EventLoopStrategy.start] conditions are met
     *
     * ## Race Condition Prevention
     *
     * The locking strategy prevents a tricky race condition: if a parent coroutine is picked up due
     * to a timeout condition becoming true, we need to guarantee that all child SEEN events have
     * actually terminated to be able to do a proper rollback. Without locking, a child could
     * register a SEEN event at the precise moment the parent starts cancellation, leading to a
     * situation where part of the saga is still executing, while part is rolling back. Since a saga
     * getting picked up also involves locking it for the entire run (see [finalSelect]), this
     * guarantees that these two operations will always happen sequentially, and either:
     * 1. A child SEEN is written first, in which case the parent saga won't get picked up for
     *    execution, since not all child SEEN's have finished (although, since deadlines are usually
     *    inherited, the child will likely immediately finish with a timeout failure), or
     * 2. The saga is picked up first, in which case a ROLLING_BACK is emitted. Then, once we try to
     *    persist a child SEEN, we will fail the `last_parent_event` condition bellow, which checks
     *    that the parent is still waiting in the step in which the message was emitted.
     *
     * ## SQL Overview
     *
     * **SEEN Creation (Normal Execution)**:
     * 1. Find EMITTED events without corresponding SEEN events
     *     - `emitted_missing_seen` CTE: Identifies gaps where handlers need to start
     * 2. Validate parent is SUSPENDED in the correct step and locks it
     *     - `parent_seen_exists`: Finds parent SEEN record
     *     - `parent_seen_lock_attempt`: Acquires lock on parent
     *     - `last_parent_event`: Validates parent is SUSPENDED with matching step
     * 3. Create SEEN events (extends cooperation lineage with new UUID)
     *     - `seen_insert` CTE: Inserts validated SEEN events with extended lineage
     *
     * **ROLLING_BACK Creation (Rollback Execution)**:
     * 1. Find ROLLBACK_EMITTED events without corresponding ROLLING_BACK events
     *     - `rollback_emitted_missing_rolling_back` CTE: Identifies rollback requests needing
     *       handlers
     * 2. Validate parent exists and lock it
     *     - `parent_seen_exists`: Finds parent SEEN record
     *     - `parent_seen_lock_attempt`: Acquires lock on parent
     * 3. Create ROLLING_BACK events (preserves existing cooperation lineage)
     *     - `rolling_back_insert` CTE: Inserts validated ROLLING_BACK events with original lineage
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
                        -- EventLoopStrategy says we're good to go
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
                                -- OR the SEEN record exists AND we successfully locked it
                                -- we don't do require that last_parent_event be a SUSPENDED, like we do when dealing with SEEN above,
                                -- because we want to allow partial rollbacks of subtrees (even though it's dangerous) 
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
     * This data class contains all the information needed to build a
     * [Continuation][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation]
     * and resume saga execution from where it left off.
     *
     * @param messageId The ID of the message being processed
     * @param topic The message topic
     * @param cooperationLineage The cooperation lineage for this saga execution
     * @param payload The message payload
     * @param createdAt When the message was originally created
     * @param step The last step that was suspended (null if not suspended yet)
     * @param latestScopeContext Under certain conditions, the context from the second-to-last
     *   persisted message event. See doc comment of [finalSelect] for an explanation.
     * @param latestContext The context from the last persisted message event
     * @param childRolledBackExceptions JSON array of exceptions from child handlers that rolled
     *   back successfully
     * @param childRollbackFailedExceptions JSON array of exceptions from child rollbacks that
     *   failed
     * @param rollingBackException The exception that caused this saga to enter rollback mode (if
     *   any)
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
     * This method uses the SQL query defined in [finalSelect] to determine readiness, and
     * implements the equivalent of double-checked locking to guard against a tricky race condition
     * (see bellow).
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
            // An important refresher: FOR UPDATE SKIP LOCKED references the state of
            // locks at the time it's executed, not at the beginning of the transaction.
            //
            // A race condition can arise in the following way:
            //
            // During the evaluation of step 1, some other handler can already be
            // processing a SEEN, but not have committed yet, so we still get "ready to be
            // processed" for that SEEN. But before we get to step 2, where the lock would
            // prevent us from picking it up, the handler commits and releases the lock.
            // This causes us to pick up the SEEN, but still retain and use the old data
            // we fetched earlier, resulting in us re-running the same step again.
            // Therefore, we need to mitigate this by rerunning the selection process once
            // we have acquired the lock.
            //
            // Postgres attempts to mitigate this to a certain extent by checking that the
            // row being locked has not been modified during the time the query is run and
            // refetching the record if that happens. Some solutions to this problem revolve
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
                            @Suppress("UNCHECKED_CAST")
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

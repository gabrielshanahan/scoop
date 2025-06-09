package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.reactive.flatMapNonNull
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.isNotEmpty
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlClient
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * MessageEventRepository - Core database operations for structured cooperation state management
 *
 * This repository handles all the database persistence logic that makes structured cooperation
 * work. It manages message events that track saga execution state, cooperation lineages, and
 * rollback coordination across distributed services.
 *
 * ## Structured Cooperation Role
 *
 * This component is fundamental to structured cooperation because it:
 * - Tracks cooperation lineages to maintain message hierarchies
 * - Enforces the suspension rule through database state queries
 * - Coordinates rollback propagation across service boundaries
 * - Prevents race conditions through careful locking strategies
 * - Manages context propagation between parent and child sagas
 *
 * ## Key Concepts
 *
 * **Message Events**: Database records that track saga lifecycle changes (EMITTED, SEEN, SUSPENDED,
 * etc.) **Cooperation Lineage**: UUID arrays that represent parent-child relationships between
 * messages **Event Types**: EMITTED, SEEN, SUSPENDED, COMMITTED, ROLLING_BACK, ROLLBACK_EMITTED,
 * etc. **Locking Strategy**: Uses FOR UPDATE SKIP LOCKED to prevent concurrent execution conflicts
 *
 * ## Integration Points
 *
 * This repository integrates with:
 * - [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] for saga execution
 *   coordination
 * - [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
 *   for execution policy enforcement
 * - [PostgresMessageQueue][io.github.gabrielshanahan.scoop.reactive.messaging.PostgresMessageQueue]
 *   for message persistence
 * - [PendingCoroutineRunSql][io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.PendingCoroutineRunSql]
 *   for readiness queries
 *
 * ## Important Behaviors
 *
 * **Race Condition Prevention**: Uses double-checked locking pattern in [fetchPendingCoroutineRun]
 * **Rollback Safety**: Only emits rollback events when no active children exist **Context
 * Propagation**: Handles serialization/deserialization of CooperationContext **Lineage Extension**:
 * Appends new UUIDs to cooperation lineages for child handlers
 *
 * @see io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
 * @see
 *   io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.PendingCoroutineRunSql
 */
@ApplicationScoped
class MessageEventRepository {

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
     * getting picked up also involves locking it for the entire run (see
     * [fetchPendingCoroutineRun]), this guarantees that these two operations will always happen
     * sequentially, and either:
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
     * @param connection Reactive SQL connection for the transaction
     * @param coroutineName Name of the coroutine to start continuations for
     * @param coroutineIdentifier Unique identifier for coroutine instances
     * @param topic Message topic this coroutine handles
     * @param eventLoopStrategy Strategy that determines when continuations should start
     * @return Uni<Unit> indicating completion of the operation
     */
    fun startContinuationsForCoroutine(
        connection: SqlConnection,
        coroutineName: String,
        coroutineIdentifier: String,
        topic: String,
        eventLoopStrategy: EventLoopStrategy,
    ): Uni<Unit> {
        val sql =
            """
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT emitted.message_id, emitted.cooperation_lineage, emitted.context, emitted.step
                    FROM message_event emitted
                    LEFT JOIN message_event AS coroutine_seen
                        ON coroutine_seen.message_id = emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = $1
                    JOIN message
                        ON message.id = emitted.message_id AND message.topic = $3
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
                        ON rolling_back_check.message_id = rollback_emitted.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = $1
                    JOIN message
                        ON message.id = rollback_emitted.message_id AND message.topic = $3
                    JOIN message_event AS coroutine_seen
                         ON coroutine_seen.message_id = rollback_emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = $1
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
                        $1,
                        $2,
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
                        $1,
                        $2,
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
        return connection
            .preparedQuery(sql)
            .execute(Tuple.of(coroutineName, coroutineIdentifier, topic))
            .replaceWith(Unit)
    }

    /**
     * Records that a message was emitted on the global scope (breaking cooperation lineage).
     *
     * Global scope emissions create new cooperation roots rather than extending existing lineages.
     * This is used when sagas deliberately want to break structured cooperation for decoupling.
     *
     * @param connection Reactive SQL connection for the transaction
     * @param messageId ID of the emitted message
     * @param cooperationLineage New cooperation lineage starting from this emission
     * @param context Optional cooperation context to propagate to independent handlers
     * @return Uni<Unit> indicating completion of the operation
     */
    fun insertGlobalEmittedEvent(
        connection: SqlConnection,
        messageId: UUID,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                )
            )
            .replaceWith(Unit)

    /**
     * Records that a message was emitted within a cooperation scope (extending the lineage).
     *
     * Scoped emissions are the normal case in structured cooperation - they create child
     * cooperation scopes that extend the parent's lineage. The emitting saga will suspend after
     * this step until all handlers of this message complete.
     *
     * @param connection Reactive SQL connection for the transaction
     * @param messageId ID of the emitted message
     * @param coroutineName Name of the saga that emitted the message
     * @param coroutineIdentifier Unique identifier for the saga instance
     * @param stepName Name of the step that emitted the message
     * @param cooperationLineage Extended cooperation lineage for child handlers
     * @param context Optional cooperation context to propagate to child handlers
     * @return Uni<Unit> indicating completion of the operation
     */
    fun insertScopedEmittedEvent(
        connection: SqlConnection,
        messageId: UUID,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context) VALUES ($1, 'EMITTED', $2, $3, $4, $5, $6)"
            )
            .execute(
                Tuple.of(
                    messageId,
                    coroutineName,
                    coroutineIdentifier,
                    stepName,
                    cooperationLineage.toTypedArray(),
                    JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                )
            )
            .replaceWith(Unit)

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
     * @param connection Reactive SQL connection for the transaction
     * @param cooperationLineage The cooperation lineage to initiate rollback for
     * @param exception The failure that triggered the rollback request
     * @return Uni<Unit> indicating completion of the operation
     */
    fun insertRollbackEmittedEvent(
        connection: SqlConnection,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH message_id_lookup AS (
                        SELECT DISTINCT message_id 
                        FROM message_event 
                        WHERE cooperation_lineage = $1
                        LIMIT 1
                    )
                    INSERT INTO message_event (
                        message_id, 
                        type,  
                        cooperation_lineage, 
                        exception
                    )
                    SELECT 
                        message_id_lookup.message_id, 'ROLLBACK_EMITTED', $1, $2
                    FROM message_id_lookup
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM message_event seen
                        LEFT JOIN message_event terminated
                          ON terminated.cooperation_lineage = seen.cooperation_lineage
                             AND terminated.type in ('COMMITTED', 'ROLLBACK_FAILED')
                        WHERE seen.type = 'SEEN'                              
                          AND seen.cooperation_lineage <@ $1 
                          AND terminated.cooperation_lineage IS NULL          
                    )
                    ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING;
                """
                    .trimIndent()
            )
            .execute(
                Tuple.tuple(
                    listOf(cooperationLineage.toTypedArray<UUID>(), JsonObject.mapFrom(exception))
                )
            )
            .replaceWith(Unit)

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
     * - **[CooperationScope.giveUpIfNecessary][io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope.giveUpIfNecessary]**:
     *   Checks for and throws cancellation exceptions during saga execution
     *
     * @param connection Reactive SQL connection for the transaction
     * @param cooperationLineage The cooperation lineage to cancel
     * @param exception The cancellation details (reason, source, etc.)
     * @return Uni<Unit> indicating completion of the operation
     * @see
     *   io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException
     * @see Capabilities.cancel
     * @see fetchGiveUpExceptions
     * @see
     *   io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.cancellationRequested
     */
    fun insertCancellationRequestedEvent(
        connection: SqlConnection,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH message_id_lookup AS (
                        SELECT DISTINCT message_id 
                        FROM message_event 
                        WHERE cooperation_lineage = $1
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
                        $1, 
                        $2
                    FROM message_id_lookup
                    ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING
                """
                    .trimIndent()
            )
            .execute(
                Tuple.tuple(
                    listOf(cooperationLineage.toTypedArray(), JsonObject.mapFrom(exception))
                )
            )
            .replaceWith(Unit)

    /**
     * Generic method for inserting message events of any type.
     *
     * This is a lower-level utility method used by more specific insertion methods. It handles the
     * general case of creating message events with all possible fields.
     *
     * @param client Reactive SQL client for the operation
     * @param messageId The ID of the message being processed
     * @param messageEventType The type of event (SEEN, SUSPENDED, COMMITTED, etc.)
     * @param coroutineName Name of the coroutine handling the message
     * @param coroutineIdentifier Unique identifier for the coroutine instance
     * @param stepName Name of the step (if applicable)
     * @param cooperationLineage The cooperation lineage for this event
     * @param exception Exception details (for rollback events)
     * @param context Cooperation context to persist
     * @return Uni<Unit> indicating completion of the operation
     */
    fun insertMessageEvent(
        client: SqlClient,
        messageId: UUID,
        messageEventType: String,
        coroutineName: String,
        coroutineIdentifier: String,
        stepName: String?,
        cooperationLineage: List<UUID>,
        exception: CooperationFailure?,
        context: CooperationContext?,
    ): Uni<Unit> =
        client
            .preparedQuery(
                "INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context) " +
                    "VALUES ($1, '$messageEventType', $2, $3, $4, $5, $6, $7)"
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        messageId,
                        coroutineName,
                        coroutineIdentifier,
                        stepName,
                        cooperationLineage.toTypedArray(),
                        JsonObject.mapFrom(exception),
                        JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                    )
                )
            )
            .replaceWith(Unit)

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
     *
     * @param connection Reactive SQL connection for the transaction
     * @param stepName Name of the step whose emissions should be rolled back
     * @param cooperationLineage The cooperation lineage of the step being rolled back
     * @param coroutineName Name of the coroutine performing the rollback
     * @param coroutineIdentifier Unique identifier for the coroutine instance
     * @param scopeStepName Name of the current rollback step scope
     * @param exception The failure that triggered the rollback
     * @param context Cooperation context for the rollback operation
     * @return Uni<Unit> indicating completion of the operation
     */
    fun insertRollbackEmittedEventsForStep(
        connection: SqlConnection,
        stepName: String,
        cooperationLineage: List<UUID>,
        coroutineName: String,
        coroutineIdentifier: String,
        scopeStepName: String?,
        exception: CooperationFailure,
        context: CooperationContext?,
    ): Uni<Unit> =
        connection
            .preparedQuery(
                """
                    WITH emitted_record AS (
                        SELECT cooperation_lineage, message_id
                        FROM message_event
                        WHERE type = 'EMITTED'
                            AND step = $1
                            AND cooperation_lineage = $2
                    )
                    INSERT INTO message_event (
                        message_id, type, 
                        cooperation_lineage, coroutine_name, coroutine_identifier, step,
                        exception, context
                    )
                    SELECT 
                        message_id, 'ROLLBACK_EMITTED', 
                        $2, $3, $4, $5, $6, $7
                    FROM emitted_record;
                """
            )
            .execute(
                Tuple.tuple(
                    listOf(
                        stepName,
                        cooperationLineage.toTypedArray(),
                        coroutineName,
                        coroutineIdentifier,
                        scopeStepName,
                        JsonObject.mapFrom(exception),
                        JsonObject.mapFrom(context?.takeIf { it.isNotEmpty() }),
                    )
                )
            )
            .replaceWith(Unit)

    /**
     * Used to implement
     * [CooperationScope.giveUpIfNecessary][io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope.giveUpIfNecessary],
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
     *
     * @param connection Reactive SQL connection for the transaction
     * @param giveUpSqlProvider Function that generates SQL to check for give-up conditions
     * @param cooperationLineage The cooperation lineage to check for give-up conditions
     * @return Uni<List<CooperationException>> containing any exceptions that should cause give-up
     */
    fun fetchGiveUpExceptions(
        connection: SqlConnection,
        giveUpSqlProvider: (String) -> String,
        cooperationLineage: List<UUID>,
    ): Uni<List<CooperationException>> =
        connection
            .preparedQuery(
                """
                    WITH seen AS (
                        SELECT * FROM message_event WHERE cooperation_lineage = $1 AND type = 'SEEN'
                    )
                    ${giveUpSqlProvider("seen")}
                """
                    .trimIndent()
            )
            .execute(Tuple.of(cooperationLineage.toTypedArray()))
            .map { rowSet ->
                rowSet.map {
                    CooperationFailure.Companion.toCooperationException(
                        it.getJsonObject("exception").mapTo(CooperationFailure::class.java)
                    )
                }
            }

    /**
     * Represents a saga execution that is ready to be resumed.
     *
     * This data class contains all the information needed to build a
     * [Continuation][io.github.gabrielshanahan.scoop.reactive.coroutine.continuation.Continuation]
     * and resume saga execution from where it left off.
     *
     * @param messageId The ID of the message being processed
     * @param topic The message topic
     * @param cooperationLineage The cooperation lineage for this saga execution
     * @param payload The message payload as JsonObject
     * @param createdAt When the message was originally created
     * @param step The last step that was suspended (null if not suspended yet)
     * @param latestScopeContext Under certain conditions, the context from the second-to-last
     *   persisted message event. See doc comment of [fetchPendingCoroutineRun] for an explanation.
     * @param latestContext The context from the last persisted message event
     * @param childRolledBackExceptions JsonArray of exceptions from child handlers that rolled back
     *   successfully
     * @param childRollbackFailedExceptions JsonArray of exceptions from child rollbacks that failed
     * @param rollingBackException The exception that caused this saga to enter rollback mode (if
     *   any)
     */
    data class PendingCoroutineRun(
        val messageId: UUID,
        val topic: String,
        val cooperationLineage: List<UUID>,
        val payload: JsonObject,
        val createdAt: OffsetDateTime,
        val step: String?,
        val latestScopeContext: JsonObject?,
        val latestContext: JsonObject?,
        val childRolledBackExceptions: JsonArray,
        val childRollbackFailedExceptions: JsonArray,
        val rollingBackException: JsonObject?,
    )

    /**
     * Finds and locks a saga execution that is ready to be resumed.
     *
     * This method uses the SQL query defined in [finalSelect] to determine readiness, and
     * implements the equivalent of double-checked locking to guard against a tricky race condition
     * (see below).
     *
     * The double-checked locking pattern is necessary because:
     * 1. First query identifies candidates and picks one to lock
     * 2. During the time between evaluation and locking, another handler might complete
     * 3. Second query after acquiring lock ensures the record is still valid for processing
     * 4. This prevents re-running steps that were already completed by concurrent handlers
     *
     * @param connection Reactive SQL connection for the transaction
     * @param coroutineName Name of the coroutine to fetch pending runs for
     * @param eventLoopStrategy Strategy that determines readiness conditions
     * @return Uni<PendingCoroutineRun?> A pending coroutine run ready for execution, or null if
     *   none are ready
     */
    fun fetchPendingCoroutineRun(
        connection: SqlConnection,
        coroutineName: String,
        eventLoopStrategy: EventLoopStrategy,
    ): Uni<PendingCoroutineRun?> =
        connection
            .preparedQuery(finalSelect(eventLoopStrategy).build())
            .execute(Tuple.of(coroutineName))
            .flatMap { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    // Nothing to do -> we're done
                    Uni.createFrom().nullItem()
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
                    connection
                        .preparedQuery(finalSelect(eventLoopStrategy, true).build())
                        .execute(Tuple.of(coroutineName, row.getUUID("id")))
                }
            }
            .flatMapNonNull { rowSet ->
                val row = rowSet.firstOrNull()
                if (row == null) {
                    // After the second fetch, we found out that the record is no longer ready for
                    // processing (i.e., the race condition described above happened)
                    Uni.createFrom().nullItem()
                } else {
                    Uni.createFrom()
                        .item(
                            PendingCoroutineRun(
                                messageId = row.getUUID("id"),
                                topic = row.getString("topic"),
                                payload = row.getJsonObject("payload"),
                                createdAt =
                                    row.getLocalDateTime("created_at").atOffset(ZoneOffset.UTC),
                                cooperationLineage =
                                    row.getArrayOfUUIDs("cooperation_lineage").toList(),
                                step = row.getString("step"),
                                latestScopeContext = row.getJsonObject("latest_scope_context"),
                                latestContext = row.getJsonObject("latest_context"),
                                childRolledBackExceptions =
                                    row.getJsonArray("child_rolled_back_exceptions"),
                                childRollbackFailedExceptions =
                                    row.getJsonArray("child_rollback_failed_exceptions"),
                                rollingBackException = row.getJsonObject("rolling_back_exception"),
                            )
                        )
                }
            }
}

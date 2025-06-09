package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import org.intellij.lang.annotations.Language

/**
 * HIC SVNT DRACONES
 *
 * An SQL query that implement structured cooperation's core logic for determining saga readiness.
 * This is the heart of Scoop.
 *
 * The queries determine when a saga is ready to proceed to its next step based on the fundamental
 * rule of structured cooperation: "sagas suspend after completing a step and don't continue until
 * all handlers of emitted messages have finished executing."
 *
 * ## Query Architecture
 *
 * The query is built from a series of Common Table Expressions (CTEs) that progressively filter and
 * analyze the message_event table to find sagas ready for execution:
 * 1. [candidateSeens]: Find SEEN events for saga runs that aren't completed yet
 * 2. [latestSuspended]: Get the latest SUSPENDED event for each candidate
 * 3. **Find child operations**: Locate child sagas triggered in latest step
 *     - [childEmissionsInLatestStep]: Find messages emitted in the latest step
 *     - [childSeens]: Find happy path continuation starts for those emissions
 *     - [childRollbackEmissionsInLatestStep]: Find rollback messages from latest step
 *     - [childRollingBacks]: Find rollback path continuation starts for those emissions
 * 4. **Check completion status**: Verify child operations are done
 *     - [terminatedChildSeens]: Filter to only completed child handlers
 *     - [terminatedChildRollingBacks]: Filter to only completed child rollbacks
 * 5. [candidateSeensWaitingToBeProcessed]: Apply structured cooperation rule & [EventLoopStrategy]
 *    constraints
 * 6. **Select and lock**: Choose one saga for execution
 *     - [seenForProcessing]: Lock and select one saga for processing
 *     - [lastTwoEvents]: Get the events necessary for correct cooperation context reconstruction
 *       (see
 *       [EventLoop.fetchSomePendingCoroutineState][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop.fetchSomePendingCoroutineState])
 *     - [finalSelect]: Gather all data needed to build the continuation
 *
 * ## EventLoopStrategy Integration
 *
 * The [EventLoopStrategy] provides SQL that determines:
 * - When to resume on happy path vs. give up
 * - When to resume on rollback path vs. give up
 * - When to start processing messages (filtering old messages, etc.)
 */

/**
 * Represents a SQL query with optional Common Table Expression dependencies.
 *
 * This data class enables building complex SQL queries as a chain of CTEs, where each CTE can
 * reference previous ones. The [build] method assembles the final query.
 */
data class SQL(val cte: SQL?, val name: String?, @Language("PostgreSQL") val sql: String)

fun SQL.appendAs(name: String?, @Language("PostgreSQL") sql: String): SQL = SQL(this, name, sql)

/**
 * Finds all `SEEN` events for sagas that are not yet completed.
 *
 * This is the foundation query that identifies saga executions that might be ready to proceed. It
 * finds SEEN events (representing active saga instances) that haven't reached a terminal state yet
 * (`COMMITTED`, `ROLLED_BACK`, or `ROLLBACK_FAILED`).
 *
 * The query handles three execution states (the last two are kept separate for clarity):
 * 1. **Happy path execution**: No rollback events, not yet committed
 * 2. **Rolling back**: Has `ROLLING_BACK` event but not yet rolled back
 * 3. **Rollback requested**: Has `ROLLBACK_EMITTED` and `ROLLING_BACK`, not yet complete
 */
val candidateSeens =
    SQL(
        cte = null,
        "candidate_seens",
        """
            SELECT DISTINCT ON (seen.id)
                seen.id, 
                seen.message_id,
                seen.cooperation_lineage,
                seen.context,
                emitted.created_at as emitted_at, 
                rollback_emitted.created_at as rollback_emitted_at
            FROM message_event seen
            JOIN message_event AS emitted
                ON seen.message_id = emitted.message_id 
                    AND emitted.type = 'EMITTED'
            LEFT JOIN message_event AS rolling_back
                ON seen.message_id = rolling_back.message_id 
                    AND rolling_back.type = 'ROLLING_BACK'
                    AND rolling_back.cooperation_lineage = seen.cooperation_lineage
            LEFT JOIN message_event AS rollback_emitted
                ON seen.message_id = rollback_emitted.message_id 
                    AND rollback_emitted.type = 'ROLLBACK_EMITTED'
            WHERE seen.coroutine_name = :coroutine_name
              AND seen.type = 'SEEN'
              AND (
              ((rollback_emitted.id IS NULL AND rolling_back.id IS NULL) AND
                NOT EXISTS (
                    SELECT 1
                    FROM message_event
                    WHERE cooperation_lineage = seen.cooperation_lineage
                      AND type = 'COMMITTED'
                ))
                OR
                (
                  (rollback_emitted.id IS NULL AND rolling_back.id IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_event
                      WHERE cooperation_lineage = seen.cooperation_lineage
                        AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
                  )
                )
                OR
                (
                  (rollback_emitted.id IS NOT NULL AND rolling_back.id IS NOT NULL)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_event
                      WHERE cooperation_lineage = seen.cooperation_lineage
                        AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
                  )
                )
              )
        """
            .trimIndent(),
    )

/**
 * Gets the latest `SUSPENDED` event for each candidate saga.
 *
 * This query finds the most recent step where each candidate saga suspended execution. The step
 * name and context from this event determine which child sagas to look for and what step the saga
 * should proceed with, should it be picked.
 */
val latestSuspended =
    candidateSeens.appendAs(
        "latest_suspended",
        """
                SELECT DISTINCT ON (message_event.message_id) message_event.cooperation_lineage, message_event.step, message_event.context, message_event.created_at
                FROM message_event
                JOIN candidate_seens ON message_event.cooperation_lineage = candidate_seens.cooperation_lineage
                WHERE message_event.type = 'SUSPENDED'
                ORDER BY message_event.message_id, message_event.created_at DESC
            """
            .trimIndent(),
    )

/**
 * Finds all messages emitted during the latest suspended step.
 *
 * This query locates the `EMITTED` events created in the step where each candidate saga most
 * recently suspended. These emissions represent the child saga runs that must complete before the
 * parent saga can proceed.
 */
val childEmissionsInLatestStep =
    latestSuspended.appendAs(
        "child_emissions_in_latest_step",
        """
                SELECT emissions.*
                FROM message_event AS emissions
                JOIN latest_suspended
                    ON emissions.cooperation_lineage = latest_suspended.cooperation_lineage
                WHERE emissions.type = 'EMITTED'
                    AND emissions.step = latest_suspended.step
            """
            .trimIndent(),
    )

/**
 * Finds child handler `SEEN` events for emissions from the latest step.
 *
 * This query locates `SEEN` events that represent child handlers starting to process messages
 * emitted in the latest suspended step. The `WHERE` conditions ensure we only get direct child
 * handlers (cooperation lineage is exactly one level deeper).
 */
val childSeens =
    childEmissionsInLatestStep.appendAs(
        "child_seens",
        """
                SELECT seen.*, child_emissions_in_latest_step.cooperation_lineage as parent_cooperation_lineage
                FROM message_event seen
                JOIN child_emissions_in_latest_step ON
                    seen.message_id = child_emissions_in_latest_step.message_id
                WHERE seen.type = 'SEEN'
                    AND seen.cooperation_lineage <> child_emissions_in_latest_step.cooperation_lineage 
                    AND child_emissions_in_latest_step.cooperation_lineage <@ seen.cooperation_lineage 
                    AND cardinality(seen.cooperation_lineage) = cardinality(child_emissions_in_latest_step.cooperation_lineage) + 1
            """
            .trimIndent(),
    )

/**
 * Filters child handlers to only those that have completed execution.
 *
 * This query identifies child SEEN events that have reached a terminal state (`COMMITTED`,
 * `ROLLED_BACK`, or `ROLLBACK_FAILED`). Only when all child handlers are terminated can the parent
 * saga proceed, according to structured cooperation rules.
 */
val terminatedChildSeens =
    childSeens.appendAs(
        "terminated_child_seens",
        """
                SELECT child_seens.*
                FROM message_event seen_terminations
                JOIN child_seens ON
                    seen_terminations.message_id = child_seens.message_id
                        AND seen_terminations.cooperation_lineage = child_seens.cooperation_lineage
                WHERE 
                    seen_terminations.type in ('COMMITTED', 'ROLLED_BACK', 'ROLLBACK_FAILED')
            """
            .trimIndent(),
    )

/**
 * Finds rollback messages emitted during the latest suspended step.
 *
 * This query locates `ROLLBACK_EMITTED` events created in the step where each candidate saga most
 * recently suspended. These represent rollback requests that child handlers must process before the
 * parent can proceed on the rollback path.
 */
val childRollbackEmissionsInLatestStep =
    terminatedChildSeens.appendAs(
        "child_rollback_emissions_in_latest_step",
        """
                SELECT rollback_emissions.*
                FROM message_event AS rollback_emissions
                JOIN latest_suspended
                    ON rollback_emissions.cooperation_lineage = latest_suspended.cooperation_lineage
                WHERE rollback_emissions.type = 'ROLLBACK_EMITTED'
                    AND rollback_emissions.step = latest_suspended.step
            """
            .trimIndent(),
    )

/**
 * Finds child handler `ROLLING_BACK` events for direct child operations.
 *
 * This query locates `ROLLING_BACK` events that represent child handlers starting to process
 * rollback requests. The `WHERE` conditions ensure we only get direct child handlers (cooperation
 * lineage is exactly one level deeper).
 */
val childRollingBacks =
    childRollbackEmissionsInLatestStep.appendAs(
        "child_rolling_backs",
        """
                SELECT
                    rolling_backs.*,
                    parent_seen.cooperation_lineage AS parent_cooperation_lineage
                FROM message_event rolling_backs
                JOIN candidate_seens AS parent_seen 
                    ON rolling_backs.cooperation_lineage <> parent_seen.cooperation_lineage 
                        AND parent_seen.cooperation_lineage <@ rolling_backs.cooperation_lineage 
                        AND cardinality(rolling_backs.cooperation_lineage) = cardinality(parent_seen.cooperation_lineage) + 1
                WHERE rolling_backs.type = 'ROLLING_BACK'
                ORDER BY rolling_backs.created_at
            """
            .trimIndent(),
    )

/**
 * Filters child rollback handlers to only those that have completed.
 *
 * This query identifies child `ROLLING_BACK` events that have reached a terminal rollback state
 * (`ROLLED_BACK` or `ROLLBACK_FAILED`). Only when all child rollbacks are terminated can the parent
 * saga proceed on the rollback path.
 */
val terminatedChildRollingBacks =
    childRollingBacks.appendAs(
        "terminated_child_rolling_backs",
        """
                SELECT child_rolling_backs.*
                FROM message_event rolling_back_terminations
                JOIN child_rolling_backs ON
                    rolling_back_terminations.message_id = child_rolling_backs.message_id
                        AND rolling_back_terminations.cooperation_lineage = child_rolling_backs.cooperation_lineage
                WHERE
                    rolling_back_terminations.type in ('ROLLED_BACK', 'ROLLBACK_FAILED')
            """
            .trimIndent(),
    )

/**
 * Applies structured cooperation rules and [EventLoopStrategy] constraints to determine saga
 * readiness.
 *
 * This is the core logic that implements structured cooperation's fundamental rule. It determines
 * which candidate sagas are ready to proceed by checking:
 *
 * **Happy Path**: All child `SEEN` events have terminated (in `terminated_child_seens`) and
 * [EventLoopStrategy] says we can [resume][EventLoopStrategy.resumeHappyPath] or
 * [give up][EventLoopStrategy.giveUpOnHappyPath]. **Rollback Path**: All child `ROLLING_BACK`
 * events have terminated (in `terminated_child_rolling_backs`) and [EventLoopStrategy] says we can
 * [resume][EventLoopStrategy.resumeRollbackPath] or
 * [give up][EventLoopStrategy.giveUpOnRollbackPath]
 *
 * The way [EventLoopStrategy] implements these methods gives rise to various features, such as
 * timeouts and cancellations.
 *
 * @see
 *   io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.StandardEventLoopStrategy
 */
fun candidateSeensWaitingToBeProcessed(eventLoopStrategy: EventLoopStrategy) =
    terminatedChildRollingBacks.appendAs(
        "candidate_seens_waiting_to_be_processed",
        """
                SELECT candidate_seens.*
                FROM candidate_seens
                WHERE
                    -- no rollback emissions present
                    (
                        NOT EXISTS ( -- no rollback emissions
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeHappyPath("candidate_seens", "child_emissions_in_latest_step", "child_seens")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnHappyPath("candidate_seens")}
                                )
                            )
                            AND 
                                NOT EXISTS ( -- every SEEN has a counterpart in terminated_child_seens
                                    SELECT 1
                                    FROM child_seens
                                    LEFT JOIN terminated_child_seens ON child_seens.cooperation_lineage = terminated_child_seens.cooperation_lineage
                                    WHERE
                                        child_seens.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_seens.cooperation_lineage IS NULL
                                )
                        )
                    )
                    OR
                    -- rollback emissions present
                    (
                        EXISTS ( -- rollback emissions present
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeRollbackPath("candidate_seens", "child_rollback_emissions_in_latest_step", "child_rolling_backs")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnRollbackPath("candidate_seens")}
                                )
                            )
                            AND 
                                NOT EXISTS ( -- every ROLLING_BACK has a counterpart in terminated_child_rolling_backs
                                    SELECT
                                        1
                                    FROM child_rolling_backs
                                    LEFT JOIN terminated_child_rolling_backs ON child_rolling_backs.cooperation_lineage = terminated_child_rolling_backs.cooperation_lineage
                                    WHERE 
                                        child_rolling_backs.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_rolling_backs.cooperation_lineage IS NULL
                                )
                        )
                    )
            """
            .trimIndent(),
    )

/**
 * Locks and selects one ready saga for processing.
 *
 * This function implements the final selection and locking step. It orders ready sagas by emission
 * time (rollbacks after normal emissions) and uses FOR UPDATE SKIP LOCKED to prevent concurrent
 * processing. The [secondRunAfterLock] parameter is tied to the double-checked locking pattern used
 * in
 * [MessageEventRepository.fetchPendingCoroutineRun][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository.fetchPendingCoroutineRun],
 * and distinguishes which "pass" we're currently running.
 */
fun seenForProcessing(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    if (!secondRunAfterLock) {
        candidateSeensWaitingToBeProcessed(eventLoopStrategy)
            .appendAs(
                "seen_for_processing",
                """
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM message_event
                JOIN candidate_seens_waiting_to_be_processed ON message_event.id = candidate_seens_waiting_to_be_processed.id
                -- We want to process things in the order they were emitted, and rollbacks always happen after emissions
                ORDER BY COALESCE(candidate_seens_waiting_to_be_processed.rollback_emitted_at, candidate_seens_waiting_to_be_processed.emitted_at), candidate_seens_waiting_to_be_processed.emitted_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """
                    .trimIndent(),
            )
    } else {
        // We need to leave out the FOR UPDATE SKIP LOCKED, since once a record is locked, it's
        // locked even for the transaction that locked it
        candidateSeensWaitingToBeProcessed(eventLoopStrategy)
            .appendAs(
                "seen_for_processing",
                """
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM candidate_seens_waiting_to_be_processed
                WHERE candidate_seens_waiting_to_be_processed.message_id = :message_id
            """
                    .trimIndent(),
            )
    }

/**
 * Retrieves the last two events for proper
 * [CooperationContext][io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext]
 * reconstruction.
 *
 * The context is always taken from the last thing the saga performed, whatever that might be. For
 * "normal" situations where we're in the middle of a saga run, that context is contained in the
 * last `SUSPENDED` record, or alternatively the `SEEN` if the saga hasn't started yet.
 *
 * However, there is one situation in which we actually need to two last steps, which is when a
 * rollback that was triggered by the *parent* (i.e., a downstream failure, and not by this saga
 * itself) is just starting.
 *
 * In those situations, the last step is a `ROLLING_BACK`, and the context contained in it is the
 * one that is "sent in from the top", i.e., from whatever triggered the rollback. However, we also
 * want to take into account whatever the context was in the *actual* last step of this saga, which,
 * in this case, is the second-to-last event. Out of necessity, that second-to-last event must be
 * `COMMITTED`.
 */
fun lastTwoEvents(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    seenForProcessing(eventLoopStrategy, secondRunAfterLock)
        .appendAs(
            "last_two_events",
            """
            SELECT
                last_two_events.context,
                last_two_events.type,
                last_two_events.step
            FROM message_event last_two_events
            JOIN seen_for_processing ON seen_for_processing.cooperation_lineage = last_two_events.cooperation_lineage
            -- See the explanation above to understand why we select these particular types
            WHERE last_two_events.type IN ('SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK')
            ORDER BY last_two_events.created_at DESC
            LIMIT 2
        """
                .trimIndent(),
        )

/**
 * Gathers all data needed to build a continuation for the selected saga.
 *
 * The only thing of note is `latest_scope_context`, which is an awkward way to express the
 * "second-to-last" context in situations where it is relevant - see [lastTwoEvents] above. In that
 * situation, this really represents the "last context used when this scope ran", while
 * `latest_context` is the last *parent* context used.
 *
 * As explained in [lastTwoEvents], the only situation we're interested in this is when:
 * - the last event was a `ROLLING_BACK`
 * - the rollback was triggered externally, i.e. not by a failure in this saga. This situation can
 *   be recognized by the `step` being `null`. This can be seen by looking at the INSERT at the end
 *   of [MessageEventRepository.startContinuationsForCoroutine], and noticing that we never specify
 *   the `step`. Contrast that with
 *   [EventLoop.markRollingBackInSeparateTransaction][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop.markRollingBackInSeparateTransaction],
 *   which is what writes those events when the failure was caused during the run of this saga.
 */
fun finalSelect(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock: Boolean = false) =
    lastTwoEvents(eventLoopStrategy, secondRunAfterLock)
        .appendAs(
            null,
            """
                SELECT
                    message.*,
                    seen_for_processing.cooperation_lineage,
                    latest_suspended.step,
                    last_event.context as latest_context,
                    CASE
                        -- See the explanation above to understand the logic behind this weird-looking selection
                        WHEN last_event.type = 'ROLLING_BACK' AND last_event.step IS NULL THEN second_to_last_event.context
                    END AS latest_scope_context,
                    (
                        SELECT
                            COALESCE(JSON_AGG(exception), '[]'::json)
                        FROM child_rolling_backs
                        JOIN seen_for_processing ON child_rolling_backs.parent_cooperation_lineage = seen_for_processing.cooperation_lineage
                    ) AS child_rolled_back_exceptions,
                    (
                        SELECT
                            COALESCE(JSON_AGG(termination_event.exception), '[]'::json)
                        FROM child_rolling_backs
                        JOIN message_event termination_event ON child_rolling_backs.cooperation_lineage = termination_event.cooperation_lineage
                        WHERE termination_event.type = 'ROLLBACK_FAILED'
                    ) AS child_rollback_failed_exceptions,
                    (
                        SELECT
                            exception
                        FROM message_event
                        JOIN seen_for_processing ON
                                message_event.cooperation_lineage = seen_for_processing.cooperation_lineage
                        WHERE message_event.type = 'ROLLING_BACK'
                          AND exception IS NOT NULL
                        LIMIT 1
                    ) AS rolling_back_exception
                FROM seen_for_processing
                LEFT JOIN latest_suspended ON seen_for_processing.cooperation_lineage = latest_suspended.cooperation_lineage
                JOIN (SELECT * FROM last_two_events LIMIT 1) last_event ON TRUE
                LEFT JOIN (SELECT * FROM last_two_events OFFSET 1 LIMIT 1) second_to_last_event ON TRUE
                JOIN message ON seen_for_processing.message_id = message.id
            """
                .trimIndent(),
        )

/** Utility function to add a comma to non-empty strings for CTE chaining. */
fun commatize(str: String) = if (str.isNotBlank()) "$str," else str

/** Utility function to add `WITH` keyword to non-empty CTE strings. */
fun withWITH(str: String) = if (str.isNotBlank()) "WITH $str" else str

/**
 * Recursively builds a CTE chain from nested SQL objects.
 *
 * This extension function converts a chain of SQL objects into a formatted CTE string, handling the
 * recursive dependency structure.
 */
fun SQL?.asCTE(): String =
    when (this) {
        null -> ""
        else ->
            """
        |${commatize(cte.asCTE())}
        |$name AS (
        |    $sql
        |)
    """
                .trimMargin()
    }

/**
 * Builds the final SQL query from a chain of CTEs.
 *
 * This function assembles the complete SQL query by combining all CTEs with the final `SELECT`
 * statement and proper SQL formatting.
 */
fun SQL.build(): String =
    """
    |${withWITH(cte.asCTE())}
    |$sql;
"""
        .trimMargin()

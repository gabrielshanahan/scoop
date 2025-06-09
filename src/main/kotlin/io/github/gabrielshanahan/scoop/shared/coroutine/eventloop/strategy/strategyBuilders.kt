package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime
import org.intellij.lang.annotations.Language

/**
 * EventLoopStrategy SQL Builder Functions
 *
 * This file contains SQL builder functions that implement common patterns for EventLoopStrategy
 * conditions. These functions generate raw SQL fragments that are injected into queries to
 * determine when sagas should resume, give up, or be filtered during execution.
 *
 * @see EventLoopStrategy
 * @see StandardEventLoopStrategy
 */
@Language("PostgreSQL")
fun ignoreHierarchiesOlderThan(ignoreOlderThan: OffsetDateTime): String =
    """
        EXISTS (
            SELECT 1 
            FROM message_event root_emission
            WHERE root_emission.type = 'EMITTED'
                AND cardinality(root_emission.cooperation_lineage) = 1
                AND root_emission.cooperation_lineage[1] = emitted.cooperation_lineage[1]
                AND root_emission.created_at >= '$ignoreOlderThan'
        )
    """
        .trimIndent()

/**
 * Generates SQL query to detect if cancellation has been requested for a saga.
 *
 * Checks if any `CANCELLATION_REQUESTED` event exists in the cooperation lineage hierarchy that
 * would affect the current saga. When cancellation is requested, the fragment returns the exception
 * details that should be thrown to abort the saga.
 *
 * Note: This checks for cancellations even in parent saga runs.
 *
 * @param seen Table alias for the `SEEN` event of the saga run being checked for cancellation
 * @return SQL query that returns the cancellation exception if cancellation was requested
 */
@Language("PostgreSQL")
fun cancellationRequested(seen: String): String =
    """
    SELECT cancellation_request.exception::jsonb
        FROM message_event cancellation_request
        JOIN $seen ON cancellation_request.cooperation_lineage <@ $seen.cooperation_lineage
        AND cancellation_request.type = 'CANCELLATION_REQUESTED'
    """
        .trimIndent()

/**
 * Generates SQL condition to check if a happy path deadline has been missed.
 *
 * Happy path deadlines apply during normal saga execution (non-rollback scenarios). The deadline is
 * checked against the most recent SUSPENDED or SEEN event for the saga.
 *
 * @param seen Table alias for the SEEN event being checked
 * @return SQL query that returns deadline missed exception if the happy path deadline expired
 */
fun happyPathDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "happy path", listOf("SUSPENDED", "SEEN"))

/**
 * Generates SQL condition to check if a rollback deadline has been missed.
 *
 * Rollback deadlines apply during compensating action execution. The deadline is checked against
 * the most recent SUSPENDED or ROLLING_BACK event for the saga.
 *
 * @param seen Table alias for the SEEN event being checked
 * @return SQL query that returns deadline missed exception if the rollback deadline expired
 */
fun rollbackDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "rollback", listOf("SUSPENDED", "ROLLING_BACK"))

/**
 * Generates SQL condition to check if an absolute deadline has been missed.
 *
 * Absolute deadlines apply to the entire saga lifecycle regardless of whether it's in happy path or
 * rollback execution. Checked against SEEN, SUSPENDED, or ROLLING_BACK events.
 *
 * @param seen Table alias for the SEEN event being checked
 * @return SQL query that returns deadline missed exception if the absolute deadline expired
 */
fun absoluteDeadlineMissed(seen: String): String =
    deadlineMissed(seen, "absolute", listOf("SEEN", "SUSPENDED", "ROLLING_BACK"))

/**
 * Core deadline checking implementation used by the specific deadline functions.
 *
 * Generates SQL that checks if a deadline of the specified type has been exceeded by comparing the
 * deadline timestamp stored in the saga's CooperationContext against the current time. Returns a
 * properly formatted CooperationFailure exception if the deadline was missed.
 *
 * ## Deadline Implementation Details
 * - Deadlines are stored in CooperationContext using keys like "HappyPathDeadlineKey"
 * - The context contains deadline timestamp, source information, and trace data
 * - Exception includes deadline source and trace for debugging missed deadlines
 * - Only checks events of the specified types (e.g., SUSPENDED, SEEN, ROLLING_BACK)
 *
 * @param seen Table alias for the SEEN event being checked
 * @param deadlineType Deadline type (e.g., "happy path", "rollback", "absolute")
 * @param eventTypes List of event types to check for deadline context
 * @return SQL query that returns deadline missed exception if deadline was exceeded
 */
@Language("PostgreSQL")
fun deadlineMissed(seen: String, deadlineType: String, eventTypes: List<String>): String {
    // Both of the following need to match the names of the classes, e.g. HappyPathDeadline and
    // HappyPathDeadlineKey
    val deadline = "${deadlineType.capitalizeWords().split(" ").joinToString("")}Deadline"
    val deadlineKey = "${deadline}Key"

    return """
        SELECT jsonb_build_object(
                'message', 'Missed $deadlineType deadline of ' || 
                    (deadline_record.context->'$deadlineKey'->>'source') || 
                    ' at ' || 
                    (deadline_record.context->'$deadlineKey'->>'deadline') || 
                    '. Deadline trace: ' || 
                    (COALESCE(to_jsonb(deadline_record.context->'$deadlineKey'->'trace')::text, '[]')),
                'type', 'Missed$deadline',
                'source', (deadline_record.context->'$deadlineKey'->>'source'),
                'stackTrace', '[]'::jsonb,
                'causes', '[]'::jsonb
            ) as exception
            FROM (
                SELECT last_event.context
                FROM message_event last_event
                JOIN $seen ON last_event.cooperation_lineage = $seen.cooperation_lineage
                WHERE jsonb_exists_any_indexed(last_event.context, '$deadlineKey')
                AND last_event.type IN ${eventTypes.asSqlList()}
                AND (last_event.context->'$deadlineKey'->>'deadline')::timestamptz < CLOCK_TIMESTAMP()
                LIMIT 1
            ) AS deadline_record
            WHERE deadline_record.context IS NOT NULL
        """
        .trimIndent()
}

private fun String.capitalizeWords() =
    split(" ").joinToString(" ") {
        it.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
    }

private fun List<String>.asSqlList() = joinToString(prefix = "(", postfix = ")") { "'$it'" }

/**
 * Generates SQL condition to verify all expected handlers have started processing emissions.
 *
 * This is a major part of structured cooperation and is at the heart of the (dummy) approach Scoop
 * uses to solve the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology)
 *
 * @param topicsToHandlerNames Map of topics to list of handler names that listen to each topic
 * @param candidateSeen Table alias for the SEEN event being evaluated for resumption
 * @param emissionInLatestStep Table alias for emissions from the saga's most recent step
 * @param emissionContinuationStart Table alias for SEEN events from child handlers
 * @return SQL condition that evaluates to true when all expected handlers have started
 * @see io.github.gabrielshanahan.scoop.blocking.messaging.HandlerRegistry
 */
@Language("PostgreSQL")
fun allEmissionsHaveCorrespondingContinuationStarts(
    topicsToHandlerNames: Map<String, List<String>>,
    candidateSeen: String,
    emissionInLatestStep: String,
    emissionContinuationStart: String,
): String {
    // Convert the handler topology map into a flat list of (topic, handler) pairs
    // This transforms Map<String, List<String>> into List<Pair<String, String>>
    val topicToHandlerPairs =
        topicsToHandlerNames.flatMap { (topic, handlers) ->
            handlers.map { handler -> topic to handler }
        }

    // If no handlers are registered, all emissions are trivially handled
    // This avoids generating invalid SQL with empty VALUES clause
    if (topicToHandlerPairs.isEmpty()) {
        return "TRUE"
    }

    // Create SQL VALUES clause like: ('topic1', 'handler1'), ('topic2', 'handler2')
    // This represents all known topic->handler relationships in the system
    val valuesClause =
        topicToHandlerPairs.joinToString(", ") { (topic, handler) -> "('$topic', '$handler')" }

    return """
        -- Implemented as "no missing SEENs exist". This is done by constructing a temporary table of
        -- all expected (topic, handler) pairs (the inside of the EXISTS clause below) and verifying
        -- that there is no such pair for which a corresponding SEEN event is missing.
        NOT EXISTS ( 
            SELECT 1
                FROM $emissionInLatestStep  -- Emissions (EMITTED reccords) from the saga's most recent step
                -- Check if ANY child handler has started processing this emission
                LEFT JOIN $emissionContinuationStart ON $emissionContinuationStart.parent_cooperation_lineage = $emissionInLatestStep.cooperation_lineage
            WHERE
                $emissionInLatestStep.cooperation_lineage = $candidateSeen.cooperation_lineage
                AND 
                -- Check if there's any expected handler that hasn't started yet
                EXISTS (
                    SELECT 1
                    -- Create a temporary table of all known (topic, handler) pairs
                    FROM (VALUES $valuesClause) AS topic_handler(topic, handler)
                    -- Find the actual message that was emitted to this topic
                    JOIN message ON topic_handler.topic = message.topic
                    -- Link back to the emission event for this message
                    JOIN $emissionInLatestStep ON $emissionInLatestStep.message_id = message.id
                    -- Try to find if this specific handler has started processing this message
                    LEFT JOIN 
                        $emissionContinuationStart ON $emissionContinuationStart.message_id = $emissionInLatestStep.message_id
                        AND $emissionContinuationStart.coroutine_name = topic_handler.handler
                    -- If the handler hasn't started (NULL id), this emission is not fully handled
                    WHERE $emissionContinuationStart.id is NULL
                )  
        )
        """
        .trimIndent()
}

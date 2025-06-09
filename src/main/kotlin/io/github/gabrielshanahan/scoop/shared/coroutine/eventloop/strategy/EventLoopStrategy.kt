package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime
import org.intellij.lang.annotations.Language

/**
 * Defines when and how sagas should be resumed or cancelled based on their current state.
 * 
 * The [EventLoopStrategy] is one of the most sophisticated components in Scoop. It provides
 * the SQL fragments that determine when a saga is ready to resume execution. This is where
 * the core logic of structured cooperation is implemented - ensuring sagas only resume when
 * all child handlers have completed.
 * 
 * ## Purpose
 * 
 * The strategy addresses several complex scenarios:
 * - **Handler topology discovery**: Determining which sagas handle which messages
 * - **Deployment coordination**: Handling cases where handlers are deployed/undeployed
 * - **Network partitions**: Dealing with temporarily unavailable handlers
 * - **Timeouts and cancellation**: Implementing deadlines and cooperative cancellation
 * 
 * ## SQL Generation
 * 
 * Each method returns SQL fragments that are incorporated into larger queries that determine
 * which sagas are ready to execute. The SQL operates on the `message` and `message_events`
 * tables to analyze the state of message hierarchies.
 * 
 * For detailed background on the challenges this solves, see:
 * https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology
 * 
 * TODO: Doc that giving up is done by scheduling the task, because giving up is checked just
 * before the coroutine is run.
 */
interface EventLoopStrategy {
    
    /**
     * Determines which emitted messages should have SEEN events created.
     * 
     * This controls the first phase of the event loop tick - finding messages that
     * have been emitted but not yet marked as "seen" by their handlers.
     * 
     * @param emitted SQL alias for the emitted message record
     * @return SQL fragment that filters which emitted messages to process
     */
    fun start(emitted: String): String

    /**
     * Determines when a saga in normal execution can resume to its next step.
     * 
     * This implements the core structured cooperation rule: a saga can only resume
     * when all handlers of messages emitted in the previous step have finished.
     * 
     * @param candidateSeen SQL alias for the SEEN event being considered for resumption
     * @param emittedInLatestStep SQL alias for messages emitted in the current step  
     * @param childSeens SQL alias for child handler SEEN events
     * @return SQL fragment that determines if the saga can resume
     */
    fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String

    /**
     * Determines when a saga in normal execution should be cancelled/give up.
     * 
     * This checks for cancellation requests, deadline violations, and other conditions
     * that should cause a saga to stop executing and enter rollback mode.
     * 
     * @param seen SQL alias for the SEEN event being checked
     * @return SQL fragment that determines if the saga should give up
     */
    fun giveUpOnHappyPath(seen: String): String

    /**
     * Determines when a saga in rollback mode can resume to its next rollback step.
     * 
     * Similar to [resumeHappyPath] but for rollback execution - determines when
     * all child rollbacks have completed so the parent can continue its own rollback.
     * 
     * @param candidateSeen SQL alias for the SEEN event being considered 
     * @param rollbacksEmittedInLatestStep SQL alias for rollback events in current step
     * @param childRollingBacks SQL alias for child rollback events
     * @return SQL fragment that determines if rollback can resume
     */
    fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String

    /**
     * Determines when a saga in rollback mode should be cancelled/give up.
     * 
     * Similar to [giveUpOnHappyPath] but for rollback execution. Generally more
     * restrictive since rollbacks are critical for maintaining system consistency.
     * 
     * @param seen SQL alias for the SEEN event being checked
     * @return SQL fragment that determines if rollback should give up
     */
    fun giveUpOnRollbackPath(seen: String): String
}

/**
 * Base implementation of [EventLoopStrategy] that provides common deadline and cancellation logic.
 * 
 * This abstract class implements the standard cancellation and deadline checking logic that
 * most event loop strategies will want. It handles:
 * - Cooperative cancellation via [CancellationToken]
 * - Absolute deadlines that apply to both happy path and rollback
 * - Happy path specific deadlines  
 * - Rollback path specific deadlines
 * - Filtering out old message hierarchies for performance
 * 
 * Subclasses only need to implement the core resumption logic for [resumeHappyPath] and
 * [resumeRollbackPath] - the give up strategies are handled here.
 * 
 * @param ignoreOlderThan Messages older than this timestamp are ignored for performance
 */
abstract class BaseEventLoopStrategy(val ignoreOlderThan: OffsetDateTime) : EventLoopStrategy {

    @Language("PostgreSQL")
    override fun start(emitted: String): String = ignoreHierarchiesOlderThan(ignoreOlderThan)

    /**
     * Standard give up conditions for happy path execution.
     * 
     * A saga will give up (enter rollback mode) if:
     * - A cancellation has been requested via [CancellationToken]
     * - A happy path deadline has been exceeded
     * - An absolute deadline has been exceeded
     * 
     * The event types checked ('SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK') represent
     * the states where a saga can be in a position to give up.
     * 
     * TODO: Doc why 'SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK'
     */
    @Language("PostgreSQL")
    override fun giveUpOnHappyPath(seen: String): String =
        """
        ${cancellationRequested(seen)}
        
        UNION ALL
        
        ${happyPathDeadlineMissed(seen)}
        
        UNION ALL
        
        ${absoluteDeadlineMissed(seen)}
        """
            .trimIndent()

    @Language("PostgreSQL")
    override fun giveUpOnRollbackPath(seen: String): String =
        """
        ${rollbackDeadlineMissed(seen)}
        
        UNION ALL
        
        ${absoluteDeadlineMissed(seen)}
    """
            .trimIndent()
}

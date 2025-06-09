package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime
import org.intellij.lang.annotations.Language

/**
 * Defines when and how sagas should be started, resumed or given up on, based on their current
 * state.
 *
 * The [EventLoopStrategy] is one of the fundamental components of Scoop. It provides the SQL
 * fragments that determine when a saga is ready to resume execution, and can be used to build a
 * wide array of features, including timeouts, cancellations, sleeping, and more.
 *
 * Apart from that, it is also the place where you need to solve the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology),
 * by implementing [resumeHappyPath]/[resumeRollbackPath] in such a way that it checks if all
 * expected handlers have reacted to all emissions in the previous step. Since this is not at all a
 * trivial problem, Scoop, as a POC, only provides a dummy solution that is built on
 * [HandlerRegistry][io.github.gabrielshanahan.scoop.blocking.messaging.HandlerRegistry] and
 * demonstrates the basic principles.
 *
 * ## SQL Generation
 *
 * Each method returns SQL fragments that are incorporated into larger queries that determine which
 * sagas are ready to execute. See
 * [finalSelect][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.finalSelect]
 *
 * For a more detailed background, see:
 * https://developer.porn/posts/implementing-structured-cooperation/#eventloopstrategy
 *
 * ## Give Up Mechanism
 *
 * Giving up is implemented by picking up the saga for execution (due to the
 * [giveUpOnHappyPath]/[giveUpOnRollbackPath] SQL fragments saying so), which then fails immediately
 * due to the
 * [giveUpIfNecessary][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.BaseCooperationContinuation.giveUpIfNecessary]
 * checks in
 * [BaseCooperationContinuation.resumeCoroutine][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.BaseCooperationContinuation.resumeCoroutine].
 */
interface EventLoopStrategy {

    /**
     * Determines which emitted messages should have SEEN events created.
     *
     * This controls the first phase of the event loop tick - finding messages that have been
     * emitted but not yet marked as "seen" by their handlers. See
     * [MessageEventRepository.startContinuationsForCoroutine][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository.startContinuationsForCoroutine].
     *
     * @param emitted The name of the CTE containing the emitted messages
     * @return SQL fragment that can be chained with `AND`/`OR`
     */
    fun start(emitted: String): String

    /**
     * Determines when a saga in normal execution can resume to its next step.
     *
     * @param candidateSeen The name of the CTE containing the SEEN event being considered for
     *   resumption
     * @param emittedInLatestStep The name of the CTE containing messages emitted in the current
     *   step
     * @param childSeens The name of the CTE containing child handler SEEN events
     * @return SQL fragment that can be chained with `AND`/`OR`
     */
    fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String

    /**
     * Determines when a saga in normal execution should be cancelled/give up.
     *
     * This checks for cancellation requests, deadline violations, and other conditions that should
     * cause a saga to stop executing and enter rollback mode.
     *
     * @param seen The name of the CTE containing the SEEN event being checked
     * @return SQL fragment that selects the `JSONB`s of
     *   [CooperationFailures][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure]
     *   representing the reason the saga is being given up on
     */
    fun giveUpOnHappyPath(seen: String): String

    /**
     * Determines when a saga in rollback mode can resume to its next rollback step.
     *
     * Similar to [resumeHappyPath] but for rollback execution - determines when all child rollbacks
     * have completed so the parent can continue its own rollback.
     *
     * @param candidateSeen The name of the CTE containing the SEEN event being considered
     * @param rollbacksEmittedInLatestStep The name of the CTE containing rollback events in current
     *   step
     * @param childRollingBacks The name of the CTE containing child rollback events
     * @return SQL fragment that can be chained with `AND`/`OR`
     */
    fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String

    /**
     * Determines when a saga in rollback mode should be cancelled/give up.
     *
     * Similar to [giveUpOnHappyPath] but for rollback execution. Generally more restrictive since
     * rollbacks are critical for maintaining system consistency.
     *
     * @param seen The name of the CTE containing the SEEN event being checked
     * @return SQL fragment that selects the `JSONB`s of
     *   [CooperationFailures][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure]
     *   representing the reason the saga is being given up on
     */
    fun giveUpOnRollbackPath(seen: String): String
}

/**
 * Base implementation of [EventLoopStrategy] that provides common deadline and cancellation logic.
 *
 * This abstract class implements the standard cancellation and deadline checking logic that most
 * event loop strategies will want. It handles:
 * - [Cancellation
 *   requests][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationCapabilities.cancel]
 *   (not to be confused with
 *   [CancellationTokens][io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken])
 * - Various deadlines (which are special cases of
 *   [CancellationTokens][io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken])
 * - A pedagogical implementation of `start`
 *
 * See
 * [HappyPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.HappyPathDeadline],
 * [RollbackPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.RollbackPathDeadline]
 * and
 * [AbsoluteDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.AbsoluteDeadline]
 * for more information about deadlines.
 *
 * Subclasses only need to implement the core resumption logic for [resumeHappyPath] and
 * [resumeRollbackPath] - the give up strategies are handled here.
 *
 * @param ignoreOlderThan Messages older than this timestamp are ignored for performance
 */
abstract class BaseEventLoopStrategy(val ignoreOlderThan: OffsetDateTime) : EventLoopStrategy {

    @Language("PostgreSQL")
    override fun start(emitted: String): String = ignoreHierarchiesOlderThan(ignoreOlderThan)

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

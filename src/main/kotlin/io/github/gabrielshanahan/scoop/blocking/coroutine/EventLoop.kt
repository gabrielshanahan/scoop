package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.agroal.pool.wrapper.ConnectionWrapper
import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.buildHappyPathContinuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.buildRollbackPathContinuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.blocking.transactional
import io.github.gabrielshanahan.scoop.blocking.whileISaySo
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.emptyContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.RollbackState
import io.github.gabrielshanahan.scoop.shared.coroutine.renderAsString
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.github.gabrielshanahan.scoop.shared.everyJittered
import io.smallrye.mutiny.Multi
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.Duration
import java.util.*
import kotlin.concurrent.thread
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.jboss.logging.Logger
import org.postgresql.jdbc.PgConnection

/**
 * The core execution engine that drives structured cooperation by resuming suspended sagas.
 *
 * The [EventLoop] is the heart of Scoop's execution model. It implements the "event loop" pattern
 * familiar from Node.js and reactive systems, but adapted for distributed message-driven sagas.
 *
 * Sagas in Scoop are modeled as a [sequence of steps][DistributedCoroutine]. Between each step,
 * execution is suspended and waits to be resumed. This requires the ability to find and resume
 * sagas that are ready to be resumed. [EventLoop] is what implements that ability, as a [tick],
 * which is run
 * [periodically, or when NOTIFIED by Postgres][io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue.subscribe].
 *
 * ## How It Works
 *
 * The EventLoop implements this by:
 * 1. **Finding ready sagas**: Queries the database to find a saga run that is ready to resume. The
 *    [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
 *    is what dictates the conditions under which this is true (see
 *    [EventLoopStrategy.resumeHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.resumeHappyPath]
 *    and
 *    [EventLoopStrategy.resumeRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy.resumeRollbackPath]).
 * 2. **Building continuations**: Construct a [Continuation] object that represent the saga's
 *    current state
 * 3. **Resuming execution**: Executes the next step of the saga
 * 4. **Updating state**: Persists the saga's new state back to the database
 *
 * ## Triggering Execution
 *
 * The event loop runs in two scenarios:
 * - **Periodically**: Using [tickPeriodically] to poll for ready sagas
 * - **On notification**: Using Postgres LISTEN/NOTIFY when new messages arrive
 *
 * Each saga has its own event loop, i.e., a separate LISTEN/NOTIFY, and a periodic process is
 * started for each saga. See
 * [PostgresMessageQueue.subscribe][io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue.subscribe]
 * to understand how this works.\
 */
@ApplicationScoped
class EventLoop(
    private val fluentJdbc: FluentJdbc,
    private val messageEventRepository: MessageEventRepository,
    private val scopeCapabilities: ScopeCapabilities,
    private val jsonbHelper: JsonbHelper,
) {
    private val logger = Logger.getLogger(javaClass)

    /**
     * Executes a single tick of the event loop for the given saga type.
     *
     * A single "tick" of the event loop does two main things:
     * 1. **Start continuations**: Find any messages that haven't been "seen" yet and mark them as
     *    such (and something similar for new rollbacks)
     * 2. **Resume sagas**: Find saga runs that are ready to proceed and execute their next step
     *
     * Both of the above are done only for the passed in [distributedCoroutine].
     *
     * ## Why "Start Continuations"?
     *
     * In structured cooperation, each saga is modeled as a sequence of steps that can be suspended
     * and resumed. A "continuation" represents the executable portion of a saga from its current
     * state to completion - essentially "what remains to be done."
     *
     * When a new message arrives that should trigger a saga, we need to "start" its continuation by
     * marking the message as "SEEN" in the database. This creates the initial execution state that
     * allows the saga to begin running. Similarly, when rollbacks are triggered, we start rollback
     * continuations, which consists of the [rollback][TransactionalStep.rollback] portions of the
     * steps, in reverse order to how they were executed.
     *
     * The term "continuation" comes from programming language theory, where it represents "the rest
     * of the computation." In Scoop's case, each saga continuation represents the remaining steps
     * that need to execute for that particular message.
     *
     * This method will process all ready sagas of the given type before returning. If no sagas are
     * ready, it returns immediately.
     *
     * @param topic The message topic this saga is subscribed to
     * @param distributedCoroutine The saga definition to execute
     */
    fun tick(topic: String, distributedCoroutine: DistributedCoroutine) {
        try {
            fluentJdbc.transactional { connection ->
                try {
                    messageEventRepository.startContinuationsForCoroutine(
                        connection,
                        distributedCoroutine.identifier.name,
                        distributedCoroutine.identifier.instance,
                        topic,
                        distributedCoroutine.eventLoopStrategy,
                    )
                } catch (e: Exception) {
                    logger.error(
                        "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] Error when starting continuations for coroutine ${distributedCoroutine.identifier}",
                        e,
                    )
                    throw e
                }
            }

            whileISaySo { repeatCount, saySo ->
                fluentJdbc.transactional { connection ->
                    try {
                        logger.info(
                            "Run number $repeatCount for topic $topic and coroutine ${distributedCoroutine.identifier}"
                        )
                        val coroutineState =
                            fetchSomePendingCoroutineState(connection, distributedCoroutine)
                        if (coroutineState == null) {
                            return@transactional
                        }
                        saySo()
                        val continuationResult =
                            resumeCoroutine(connection, distributedCoroutine, coroutineState)

                        if (continuationResult is Continuation.ContinuationResult.Failure) {
                            throw continuationResult.exception
                        }
                    } catch (e: Exception) {
                        logger.error(
                            "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] Error when running coroutine ${distributedCoroutine.identifier}",
                            e,
                        )
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error in when ticking", e)
        }
    }

    /**
     * Starts a periodic event loop that ticks at regular intervals.
     *
     * This creates a background process that will call [tick] approximately every
     * [runApproximatelyEvery] duration. The interval is jittered to avoid
     * [thundering herd problems](https://en.wikipedia.org/wiki/Thundering_herd_problem) when
     * multiple instances are running.
     *
     * @param topic The message topic this saga handles
     * @param distributedCoroutine The saga definition to execute
     * @param runApproximatelyEvery How often to tick (with jitter applied)
     * @return An [AutoCloseable] that can be used to stop the periodic ticking
     */
    fun tickPeriodically(
        topic: String,
        distributedCoroutine: DistributedCoroutine,
        runApproximatelyEvery: Duration,
    ): AutoCloseable {
        val eventLoop =
            Multi.createFrom()
                .ticks()
                .everyJittered(runApproximatelyEvery)
                .invoke { _ ->
                    logger.info(
                        "Starting tick for topic $topic and coroutine ${distributedCoroutine.identifier}"
                    )
                    tick(topic, distributedCoroutine)
                }
                .subscribe()
                .with(
                    {},
                    { e ->
                        logger.error("Event loop for ${distributedCoroutine.identifier} failed", e)
                    },
                )

        return AutoCloseable { eventLoop.cancel() }
    }

    /**
     * Fetches the execution state for a saga that's ready to be resumed.
     *
     * ## Saga State Overview
     *
     * This method constructs a [CoroutineState] that captures where a saga is in its lifecycle. The
     * state consists of two key dimensions:
     *
     * ### Execution Progress ([LastSuspendedStep])
     * - **NotSuspendedYet**: Brand new saga that hasn't executed any steps yet
     * - **SuspendedAfter(stepName)**: Saga that completed a step and is waiting to proceed
     *
     * ### Rollback Status ([RollbackState])
     * - **Gucci**: Everything is fine, proceeding normally
     * - **SuccessfullyRolledBackLastStep**: This saga successfully completed rolling back a step
     *   that had previously failed (either the step itself failed, or it was rolled back due to
     *   child failures). This is like "Gucci" but for rollback execution - ready to continue
     *   rolling back, which means rolling back the previous step.
     * - **ChildrenFailedAndSuccessfullyRolledBack**: Child sagas failed, but their rollbacks
     *   succeeded
     * - **ChildrenFailedAndFailedToRollBack**: Child sagas failed AND their rollbacks also failed
     * - **ChildrenFailedWhileRollingBackLastStep**: Complex failure during rollback of this saga's
     *   step
     *
     * **Note on the meaning of "Children"**: In structured cooperation, when a saga step emits
     * messages via `scope.launch()`, the handlers of those messages become "child sagas" of this
     * saga. The parent saga waits for all children to complete before proceeding to its next step.
     * If any child fails, that failure propagates back to the parent, potentially triggering
     * rollbacks.
     *
     * ## State Combinations
     *
     * These dimensions combine to determine what the saga should do next:
     * - **(NotSuspendedYet, Gucci)**: Execute the first step
     * - **(SuspendedAfter, Gucci)**: Execute the next step
     * - **(SuspendedAfter, SuccessfullyRolledBackLastStep)**: Continue rolling back
     * - **(SuspendedAfter, ChildrenFailed...)**:
     *   [Handle child failures][TransactionalStep.handleChildFailures] and potentially start
     *   rolling back (or failing the rollback if one is in progress)
     *
     * This state is used to build the appropriate continuation for resuming execution.
     */
    private fun fetchSomePendingCoroutineState(
        connection: Connection,
        distributedCoroutine: DistributedCoroutine,
    ): CoroutineState? {

        // Query the database for a saga execution that's ready to be resumed.
        // This uses a moderately complex SQL query that finds a saga that is
        // ready to be resumed (dictated by the EventLoopStrategy) and fetches
        // various pieces of data that we use bellow
        val result =
            messageEventRepository.fetchPendingCoroutineRun(
                connection,
                distributedCoroutine.identifier.name,
                distributedCoroutine.eventLoopStrategy,
            )

        if (result == null) {
            logger.info(
                "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                    "No messages for coroutine ${distributedCoroutine.identifier}"
            )
            return null
        } else {
            logger.info(
                "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                    "Processing message for coroutine " +
                    "${distributedCoroutine.identifier}: " +
                    "id=${result.messageId}"
            )

            // Build the Message object from the original message data (from the 'message' table)
            // This is the message that originally triggered this saga execution
            val message =
                Message(
                    id = result.messageId,
                    topic = result.topic,
                    payload = result.payload,
                    createdAt = result.createdAt,
                )

            // Exceptions from child sagas that successfully rolled back (runs that finished with
            // ROLLED_BACK), if any
            val childRolledBackExceptions =
                jsonbHelper
                    .fromPGobjectToList<CooperationFailure>(result.childRolledBackExceptions)
                    .map(CooperationFailure::toCooperationException)

            // Exceptions from child rollbacks that failed (runs that finished with
            // ROLLBACK_FAILED), if any
            val childRollbackFailedExceptions =
                jsonbHelper
                    .fromPGobjectToList<CooperationFailure>(result.childRollbackFailedExceptions)
                    .map(CooperationFailure::toCooperationException)

            // The exception that caused THIS saga run to enter rollback mode (from the ROLLING_BACK
            // event
            // for this saga's run), if any
            val rollingBackException: CooperationException? =
                result.rollingBackException?.let {
                    CooperationFailure.toCooperationException(
                        jsonbHelper.fromPGobject<CooperationFailure>(it)
                    )
                }

            // Context handling - two types of context are tracked separately. This only becomes
            // relevant in situations where
            // the current saga actually completed correctly in the past. However, then a rollback
            // happened somewhere down the
            // line, e.g., due to a parent saga triggering it, so we've come back to this saga run
            // to roll it back, and we're
            // just about to execute the first rollback step.
            // In this specific situation, there are actually two places we want to take the context
            // from:
            // 1) the context at the end of our successful run
            // 2) the context of whatever triggered the rollback (either the parent saga or the user
            // manually)

            // Represents 1) from the description above
            // Only populated during the first rollback step of a previously successfully finished
            // saga run
            val latestScopeContext =
                result.latestScopeContext?.let { jsonbHelper.fromPGobject<CooperationContext>(it) }
                    ?: emptyContext()

            // Represents 2) in the description above Local context for this specific saga execution
            // (from last_event.context - the most recent SEEN/SUSPENDED/COMMITTED/ROLLING_BACK
            // event)
            val latestContext =
                result.latestContext?.let { jsonbHelper.fromPGobject<CooperationContext>(it) }
                    ?: emptyContext()

            // Determine rollback state based on the combination of exceptions
            // This logic maps the raw exception data to structured rollback states
            val rollbackState =
                when {
                    // Case 1: Some child rollbacks failed
                    childRollbackFailedExceptions.isNotEmpty() ->
                        if (rollingBackException == null) {
                            // Children failed, and their rollbacks also failed, but this saga
                            // hasn't started rolling back yet
                            RollbackState.ChildrenFailedAndFailedToRollBack(
                                result.step!!, // The step where children were emitted
                                childRollbackFailedExceptions,
                                childRolledBackExceptions,
                            )
                        } else {
                            // We're rolling back, but children rollbacks failed
                            RollbackState.ChildrenFailedWhileRollingBackLastStep(
                                result.step!!, // The step that was being rolled back
                                childRollbackFailedExceptions,
                                rollingBackException,
                            )
                        }

                    // Case 2: Children failed but all their rollbacks succeeded
                    childRolledBackExceptions.isNotEmpty() ->
                        if (rollingBackException == null) {
                            // Children failed and successfully rolled back, now this saga's step
                            // needs to handle it using Step.handleChildFailures,
                            // or fail itself
                            RollbackState.ChildrenFailedAndSuccessfullyRolledBack(
                                result.step!!, // The step where children were emitted
                                childRolledBackExceptions,
                            )
                        } else {
                            // This saga successfully rolled back its last step after child failures
                            RollbackState.SuccessfullyRolledBackLastStep(rollingBackException)
                        }

                    // Case 3: No child failures
                    else -> {
                        if (rollingBackException == null) {
                            // Normal execution, no failures anywhere
                            RollbackState.Gucci
                        } else {
                            // There were no children to roll back in the previous step,
                            // or a rollback just started
                            RollbackState.SuccessfullyRolledBackLastStep(rollingBackException)
                        }
                    }
                }

            return CoroutineState(
                message,

                // From latest_suspended.step (the last SUSPENDED event) null means this is a
                // brand new saga that hasn't executed any steps yet
                if (result.step == null) LastSuspendedStep.NotSuspendedYet
                else LastSuspendedStep.SuspendedAfter(result.step),

                // The cooperation lineage from the SEEN event for this saga. This lineage
                // extends the parent's lineage and identifies this saga's position in the
                // cooperation hierarchy
                CooperationScopeIdentifier.Child(result.cooperationLineage),

                // If there are conflicting values for a key in these contexts, we want the context
                // from the
                // rollback trigger (i.e., latestContext) to take precedence, since that comes
                // chronologically
                // later
                latestScopeContext + latestContext,
                rollbackState,
            )
        }
    }

    /**
     * Resumes execution of a saga from its current state and handles the transactional lifecycle.
     *
     * This method is the heart of saga execution - it takes a saga state and runs the next step,
     * handling both happy path and rollback scenarios. The method is responsible for:
     * 1. Building the appropriate continuation (happy path or rollback). See the doc comment for
     *    [tick] if you don't understand the word "continuation"
     * 2. Executing the saga step within the current transaction
     * 3. Handling the result and persisting state changes
     *
     * ## Transaction Boundaries and State Persistence
     *
     * This method operates within the transaction started by [tick]. The saga step execution
     * happens in this same transaction, which means:
     * - **Step execution is transactional**: If the step throws an exception, all database changes
     *   (including any messages emitted) are rolled back
     * - **State updates are atomic**: The step's business logic and its state updates happen
     *   atomically - either both succeed or both fail
     * - **Message emission is part of the transaction**: Any `scope.launch()` calls within the step
     *   are committed as part of the same transaction
     *
     * ## Rollback vs. Failure Handling
     *
     * When a step fails, this method distinguishes between:
     * - **Normal execution failures**: Triggers rollback mode using separate transactions
     * - **Rollback execution failures**: Marks rollback as failed using separate transactions
     *
     * The separate transactions for rollback state changes are necessary because the original
     * transaction is going to be rolled back due to the failure. These are handled by
     * [markRollingBackInSeparateTransaction] and [markRollbackFailedInSeparateTransaction].
     *
     * ## Continuation Types
     *
     * Depending on the rollback state, this method builds either:
     * - **HappyPathContinuation**: For normal forward execution
     * - **RollbackPathContinuation**: For executing compensating actions in reverse order
     *
     * @param connection The database connection for the current transaction
     * @param distributedCoroutine The saga definition containing steps and configuration
     * @param coroutineState The current execution state (step position, context, rollback status)
     * @return The result of continuation execution (Success, Failure, or Suspend)
     */
    private fun resumeCoroutine(
        connection: Connection,
        distributedCoroutine: DistributedCoroutine,
        coroutineState: CoroutineState,
    ): Continuation.ContinuationResult {
        val cooperativeContinuation =
            if (coroutineState.rollbackState is RollbackState.Me.RollingBack) {
                distributedCoroutine.buildRollbackPathContinuation(
                    connection,
                    coroutineState,
                    scopeCapabilities,
                )
            } else {
                distributedCoroutine.buildHappyPathContinuation(
                    connection,
                    coroutineState,
                    scopeCapabilities,
                )
            }

        val input =
            when (coroutineState.rollbackState) {
                is RollbackState.Gucci ->
                    Continuation.LastStepResult.SuccessfullyInvoked(coroutineState.message)

                is RollbackState.SuccessfullyRolledBackLastStep ->
                    Continuation.LastStepResult.SuccessfullyRolledBack(
                        coroutineState.message,
                        coroutineState.rollbackState.throwable,
                    )

                is RollbackState.Children.Rollbacks ->
                    Continuation.LastStepResult.Failure(
                        coroutineState.message,
                        coroutineState.rollbackState.throwable,
                    )
            }

        val continuationResult = cooperativeContinuation.resumeWith(input)

        when (continuationResult) {
            is Continuation.ContinuationResult.Success ->
                when (coroutineState.rollbackState) {
                    is RollbackState.Me.NotRollingBack ->
                        markCommited(cooperativeContinuation, coroutineState.message.id)

                    is RollbackState.Me.RollingBack ->
                        markRolledBack(cooperativeContinuation, coroutineState.message.id)
                }

            is Continuation.ContinuationResult.Failure ->
                when (coroutineState.rollbackState) {
                    is RollbackState.Me.NotRollingBack ->
                        markRollingBackInSeparateTransaction(
                            cooperativeContinuation,
                            coroutineState.message.id,
                            continuationResult.exception,
                        )

                    is RollbackState.Me.RollingBack -> {
                        markRollbackFailedInSeparateTransaction(
                            cooperativeContinuation,
                            coroutineState.message.id,
                            continuationResult.exception,
                        )
                    }
                }

            is Continuation.ContinuationResult.Suspend ->
                markSuspended(cooperativeContinuation, coroutineState.message.id)
        }

        logger.info(
            "[${(connection as ConnectionWrapper).unwrap(PgConnection::class.java).backendPID}] " +
                "Finished processing message for continuation " +
                "${cooperativeContinuation.continuationIdentifier}: " +
                "id=${coroutineState.message.id} with $continuationResult"
        )

        return continuationResult
    }

    private fun markCommited(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "COMMITTED")

    private fun markRolledBack(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "ROLLED_BACK")

    private fun markSuspended(scope: CooperationScope, messageId: UUID) =
        mark(scope, scope.connection, messageId, "SUSPENDED")

    private fun markRollingBackInSeparateTransaction(
        scope: CooperationScope,
        messageId: UUID,
        exception: Throwable? = null,
    ) =
        thread {
                fluentJdbc.transactional { connection ->
                    mark(scope, connection, messageId, "ROLLING_BACK", exception)
                }
            }
            .join()

    private fun markRollbackFailedInSeparateTransaction(
        scope: CooperationScope,
        messageId: UUID,
        exception: Throwable? = null,
    ) =
        thread {
                fluentJdbc.transactional { connection ->
                    mark(scope, connection, messageId, "ROLLBACK_FAILED", exception)
                }
            }
            .join()

    private fun mark(
        scope: CooperationScope,
        connection: Connection,
        messageId: UUID,
        messageEventType: String,
        exception: Throwable? = null,
    ) {
        val cooperationFailure =
            exception?.let {
                CooperationFailure.Companion.fromThrowable(
                    it,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                        .renderAsString(),
                )
            }

        return messageEventRepository.insertMessageEvent(
            connection,
            messageId,
            messageEventType,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            scope.scopeIdentifier.cooperationLineage,
            cooperationFailure,
            scope.context,
        )
    }
}

/**
 * Represents the execution state of a saga at a specific point in time.
 *
 * This data class captures everything needed to resume a saga from where it left off:
 * - What message triggered the saga
 * - What step it was executing last
 * - What cooperation scope it's running in
 * - Any shared context data
 * - Whether it's in normal execution or rollback mode
 *
 * The EventLoop uses this state to build the appropriate [Continuation] for resuming execution.
 *
 * @param message The message that triggered this saga instance
 * @param lastSuspendedStep The last step that completed, or [LastSuspendedStep.NotSuspendedYet] for
 *   new sagas
 * @param scopeIdentifier The cooperation scope this saga is running in
 * @param cooperationContext Shared context data (deadlines, cancellation tokens, custom data)
 * @param rollbackState Whether the saga is rolling back and what exceptions caused it
 */
data class CoroutineState(
    val message: Message,
    val lastSuspendedStep: LastSuspendedStep,
    val scopeIdentifier: CooperationScopeIdentifier.Child,
    val cooperationContext: CooperationContext,
    val rollbackState: RollbackState,
)

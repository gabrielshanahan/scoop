package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.reactive.messaging.MessageRepository
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.emptyContext
import io.github.gabrielshanahan.scoop.shared.coroutine.renderAsString
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.RollbackRequestedException
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import jakarta.enterprise.context.ApplicationScoped

/**
 * Represents the root of an independent cooperation hierarchy.
 *
 * A [CooperationRoot] is returned when launching messages that start new, independent cooperation
 * lineages (via [launchOnGlobalScope][ScopeCapabilities.launchOnGlobalScope]).
 *
 * @param cooperationScopeIdentifier The root identifier for the new independent hierarchy
 * @param message The message that was emitted to start this independent hierarchy
 */
data class CooperationRoot(
    val cooperationScopeIdentifier: CooperationScopeIdentifier.Root,
    val message: Message,
)

/**
 * Capabilities available to sagas during execution - essentially what is available to you via the
 * `scope` parameter.
 *
 * ScopeCapabilities provides the fundamental operations that sagas can perform during their
 * execution. These capabilities are used by [CooperationScope] to implement the user-facing saga
 * API and by continuation implementations to manage saga lifecycle.
 *
 * ## Relationship to [StructuredCooperationCapabilities]
 * - **ScopeCapabilities**: Operations performed from *within* a running saga
 * - **StructuredCooperationCapabilities**: Operations performed from the outside
 *
 * The key distinction is that ScopeCapabilities methods take a [CooperationScope] parameter and
 * operate within the context of that scope, while StructuredCooperationCapabilities methods operate
 * on arbitrary cooperation lineages.
 *
 * @see StructuredCooperationCapabilities for external saga management operations
 */
interface ScopeCapabilities {
    /**
     * Emits a message within the current cooperation scope. The emitted message becomes a "child"
     * of the current saga, and the saga will suspend execution after completing its current step
     * until all handlers of this message finish.
     *
     * @param scope The cooperation scope from which this message is being emitted
     * @param topic The message topic/queue name
     * @param payload The message payload (must be a JSON object)
     * @param additionalContext Optional context to combine with scope's context for the child
     * @return The emitted message record
     */
    fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext?,
    ): Uni<Message>

    /**
     * Emits a message that starts an independent cooperation hierarchy.
     *
     * Unlike [launch], this creates a completely independent message that is NOT part of the
     * current cooperation scope. The current saga will NOT wait for handlers of this message to
     * complete.
     *
     * @param scope The cooperation scope from which this independent message is being emitted
     * @param topic The message topic/queue name
     * @param payload The message payload (must be a JSON object)
     * @param context Optional context for the independent handler (not merged with current context)
     * @return A cooperation root representing the new independent hierarchy
     */
    fun launchOnGlobalScope(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot>

    /**
     * Triggers rollback for all messages emitted from a specific step.
     *
     * This method is called during rollback continuation execution to initiate compensating actions
     * in all child handlers that were spawned from a particular step.
     *
     * @param scope The cooperation scope requesting the rollback
     * @param stepName The name of the step whose emissions should be rolled back
     * @param throwable The original exception that triggered the rollback
     */
    fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable,
    ): Uni<Unit>

    /**
     * Checks if the saga should give up execution and throws an exception if so.
     *
     * This method queries the database using the provided SQL to determine if any cancellation
     * conditions have been met. If cancellation criteria are satisfied, it creates and throws a
     * [GaveUpException] to terminate saga execution.
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
     * @param scope The cooperation scope to check for cancellation
     * @param giveUpSqlProvider Function that generates SQL to check cancellation conditions
     * @throws GaveUpException if any cancellation conditions are met
     */
    fun giveUpIfNecessary(scope: CooperationScope, giveUpSqlProvider: (String) -> String): Uni<Unit>
}

/**
 * External management capabilities for sagas.
 *
 * [StructuredCooperationCapabilities] provides operations for managing sagas from outside their
 * execution context. These are "system-level" operations that operate on arbitrary cooperation
 * lineages.
 *
 * ## Relationship to [ScopeCapabilities]
 * - **ScopeCapabilities**: Operations performed from *within* a running saga
 * - **StructuredCooperationCapabilities**: Operations performed from the outside
 *
 * @see ScopeCapabilities for in-saga execution operations
 */
interface StructuredCooperationCapabilities {
    /**
     * Starts a new independent cooperation hierarchy from outside any saga context.
     *
     * This is the "entry point" for creating root-level sagas that are not spawned from other
     * sagas.
     *
     * @param connection Database connection for the operation
     * @param topic The message topic/queue name
     * @param payload The message payload (must be a JSON object)
     * @param context Optional initial context for the saga hierarchy
     * @return A cooperation root representing the new independent hierarchy
     */
    fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot>

    /**
     * Requests cancellation of a saga from outside its execution context.
     *
     * This creates a CANCELLATION_REQUESTED event that will, when used with an instance of
     * [BaseEventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.BaseEventLoopStrategy],
     * cause the saga to be canceled when it next checks for cancellation. This is a
     * *user-initiated* cancellation (e.g., someone clicks a "cancel" button) as opposed to
     * system-initiated cancellation via
     * [io.github.gabrielshanahan.scoop.shared.coroutine.context.CancellationToken].
     *
     * ## Cancellation Processing
     *
     * The cancellation is processed when:
     * - [EventLoop][io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop] checks for
     *   cancellation before executing steps
     * - [CooperationScope.giveUpIfNecessary] is called manually (always happens just before and
     *   just after a step executes)
     * - [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
     *   giveUp methods detect the cancellation request
     *
     * @param connection Database connection for recording the cancellation request
     * @param cooperationScopeIdentifier The scope identifier of the saga to cancel
     * @param source Human-readable description of what/who requested cancellation
     * @param reason Human-readable reason for the cancellation
     */
    fun cancel(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit>

    /**
     * Requests rollback of a completed saga from outside its execution context.
     *
     * This creates a ROLLBACK_EMITTED event that triggers the compensating actions for a saga that
     * has already completed successfully. This enables "undo" functionality for distributed
     * operations.
     *
     * ## Rollback vs Cancel
     * - **Cancel**: Stop a running saga while it's still running
     * - **Rollback**: Undo a saga that has already completed successfully
     *
     * @param connection Database connection for recording the rollback request
     * @param cooperationScopeIdentifier The scope identifier of the saga to roll back
     * @param source Human-readable description of what/who requested rollback
     * @param reason Human-readable reason for the rollback
     */
    fun rollback(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit>
}

/** Default implementation of both [ScopeCapabilities] and [StructuredCooperationCapabilities]. */
@ApplicationScoped
private class Capabilities(
    private val messageRepository: MessageRepository,
    private val messageEventRepository: MessageEventRepository,
) : ScopeCapabilities, StructuredCooperationCapabilities {
    override fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot> =
        messageRepository.insertMessage(connection, topic, payload).flatMap { message ->
            val cooperationId = UuidCreator.getTimeOrderedEpoch()
            val cooperationLineage = listOf(cooperationId)
            messageEventRepository
                .insertGlobalEmittedEvent(connection, message.id, cooperationLineage, context)
                .replaceWith(
                    CooperationRoot(CooperationScopeIdentifier.Root(cooperationId), message)
                )
        }

    override fun launchOnGlobalScope(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?,
    ): Uni<CooperationRoot> = launchOnGlobalScope(scope.connection, topic, payload, context)

    override fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext?,
    ): Uni<Message> =
        messageRepository.insertMessage(scope.connection, topic, payload).flatMap { message ->
            messageEventRepository
                .insertScopedEmittedEvent(
                    scope.connection,
                    message.id,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
                    scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                        .instance,
                    scope.continuation.continuationIdentifier.stepName,
                    scope.scopeIdentifier.cooperationLineage,
                    scope.context + (additionalContext ?: emptyContext()),
                )
                .invoke { _ -> scope.emitted(message) }
                .replaceWith(message)
        }

    override fun cancel(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        return messageEventRepository.insertCancellationRequestedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    override fun rollback(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit> {
        val exception = RollbackRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        return messageEventRepository.insertRollbackEmittedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    override fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable,
    ): Uni<Unit> {
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                ParentSaidSoException(throwable),
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                    .renderAsString(),
            )

        return messageEventRepository.insertRollbackEmittedEventsForStep(
            scope.connection,
            stepName,
            scope.scopeIdentifier.cooperationLineage,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            cooperationFailure,
            scope.context,
        )
    }

    override fun giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (String) -> String,
    ): Uni<Unit> =
        messageEventRepository
            .fetchGiveUpExceptions(
                scope.connection,
                giveUpSqlProvider,
                scope.scopeIdentifier.cooperationLineage,
            )
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .map { exceptions ->
                if (exceptions.any()) {
                    throw GaveUpException(exceptions)
                }
            }
}

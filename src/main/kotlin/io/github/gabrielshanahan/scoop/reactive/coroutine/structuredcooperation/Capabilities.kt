package io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.flatMapNonNull
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
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.mutiny.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

data class CooperationRoot(
    val cooperationScopeIdentifier: CooperationScopeIdentifier.Root,
    val message: Message,
)

interface ScopeCapabilities {
    fun launch(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        additionalContext: CooperationContext?,
    ): Uni<Message>

    fun launchOnGlobalScope(
        scope: CooperationScope,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?
    ): Uni<CooperationRoot>

    fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable
    ): Uni<Unit>

    fun giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (String) -> String
    ): Uni<Unit>
}

interface StructuredCooperationCapabilities {
    fun launchOnGlobalScope(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext?
    ): Uni<CooperationRoot>

    fun cancel(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit>

    fun rollback(
        connection: SqlConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ): Uni<Unit>
}

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
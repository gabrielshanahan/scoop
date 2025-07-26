package io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation

import com.github.f4b6a3.uuid.UuidCreator
import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.blocking.messaging.MessageRepository
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.context.emptyContext
import io.github.gabrielshanahan.scoop.shared.coroutine.renderAsString
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CancellationRequestedException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.GaveUpException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.ParentSaidSoException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.RollbackRequestedException
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import org.postgresql.util.PGobject

data class CooperationRoot(
    val cooperationScopeIdentifier: CooperationScopeIdentifier.Root,
    val message: Message,
)

interface ScopeCapabilities {
    fun launch(
        scope: CooperationScope,
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext?,
    ): Message

    fun launchOnGlobalScope(
        scope: CooperationScope,
        topic: String,
        payload: PGobject,
        context: CooperationContext?
    ): CooperationRoot

    fun emitRollbacksForEmissions(
        scope: CooperationScope,
        stepName: String,
        throwable: Throwable
    )

    fun giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (String) -> String
    )
}

interface StructuredCooperationCapabilities {
    fun launchOnGlobalScope(
        connection: Connection,
        topic: String,
        payload: PGobject,
        context: CooperationContext?
    ): CooperationRoot

    fun cancel(
        connection: Connection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    )

    fun rollback(
        connection: Connection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    )
}

@ApplicationScoped
private class Capabilities(
    private val messageRepository: MessageRepository,
    private val messageEventRepository: MessageEventRepository,
) : ScopeCapabilities, StructuredCooperationCapabilities {
    override fun launchOnGlobalScope(
        connection: Connection,
        topic: String,
        payload: PGobject,
        context: CooperationContext?,
    ): CooperationRoot {
        val message = messageRepository.insertMessage(connection, topic, payload)
        val cooperationId = UuidCreator.getTimeOrderedEpoch()
        val cooperationLineage = listOf(cooperationId)
        messageEventRepository.insertGlobalEmittedEvent(
            connection,
            message.id,
            cooperationLineage,
            context,
        )

        return CooperationRoot(CooperationScopeIdentifier.Root(cooperationId), message)
    }

    override fun launchOnGlobalScope(
        scope: CooperationScope,
        topic: String,
        payload: PGobject,
        context: CooperationContext?,
    ): CooperationRoot = launchOnGlobalScope(scope.connection, topic, payload, context)

    override fun launch(
        scope: CooperationScope,
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext?,
    ): Message {
        val message = messageRepository.insertMessage(scope.connection, topic, payload)

        messageEventRepository.insertScopedEmittedEvent(
            scope.connection,
            message.id,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            scope.scopeIdentifier.cooperationLineage,
            scope.context + (additionalContext ?: emptyContext()),
        )

        scope.emitted(message)

        return message
    }

    override fun cancel(
        connection: Connection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ) {
        val exception = CancellationRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        messageEventRepository.insertCancellationRequestedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    override fun rollback(
        connection: Connection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: String,
        reason: String,
    ) {
        val exception = RollbackRequestedException(reason)
        val cooperationFailure = CooperationFailure.fromThrowable(exception, source)

        messageEventRepository.insertRollbackEmittedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    override fun emitRollbacksForEmissions(scope: CooperationScope, stepName: String, throwable: Throwable) {
        val cooperationFailure =
            CooperationFailure.fromThrowable(
                ParentSaidSoException(throwable),
                scope.continuation.continuationIdentifier.distributedCoroutineIdentifier
                    .renderAsString(),
            )

        messageEventRepository.insertRollbackEmittedEventsForStep(
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

    override fun giveUpIfNecessary(scope: CooperationScope, giveUpSqlProvider: (String) -> String) {
        val exceptions =
            messageEventRepository.fetchGiveUpExceptions(
                scope.connection,
                giveUpSqlProvider,
                scope.scopeIdentifier.cooperationLineage,
            )

        if (exceptions.any()) {
            throw GaveUpException(exceptions)
        }
    }

}

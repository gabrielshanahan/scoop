package io.github.gabrielshanahan.scoop.quarkus

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.JsonbHelper
import io.github.gabrielshanahan.scoop.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.Capabilities
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.MessageEventRepository
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ReturnValueRepository
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.messaging.MessageRepository
import io.github.gabrielshanahan.scoop.messaging.PostgresMessageQueue
import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.codejargon.fluentjdbc.api.FluentJdbc

/**
 * CDI producer that creates scoop-core components using Quarkus-managed beans.
 *
 * This producer wires the framework-agnostic scoop-core components using Quarkus-provided
 * dependencies (FluentJdbc, ObjectMapper, PgSubscriber).
 */
@ApplicationScoped
class ScoopProducer(
    private val fluentJdbc: FluentJdbc,
    private val objectMapper: ObjectMapper,
    private val pgSubscriber: PgSubscriber,
) {

    @Produces @ApplicationScoped fun jsonbHelper(): JsonbHelper = JsonbHelper(objectMapper)

    @Produces
    @ApplicationScoped
    fun messageRepository(): MessageRepository = MessageRepository(fluentJdbc)

    @Produces
    @ApplicationScoped
    fun messageEventRepository(jsonbHelper: JsonbHelper): MessageEventRepository =
        MessageEventRepository(jsonbHelper, fluentJdbc)

    @Produces
    @ApplicationScoped
    fun returnValueRepository(): ReturnValueRepository = ReturnValueRepository(fluentJdbc)

    @Produces
    @ApplicationScoped
    fun scopeCapabilities(
        messageRepository: MessageRepository,
        messageEventRepository: MessageEventRepository,
        returnValueRepository: ReturnValueRepository,
    ): ScopeCapabilities =
        Capabilities(messageRepository, messageEventRepository, returnValueRepository)

    @Produces
    @ApplicationScoped
    fun structuredCooperationCapabilities(
        messageRepository: MessageRepository,
        messageEventRepository: MessageEventRepository,
        returnValueRepository: ReturnValueRepository,
    ): StructuredCooperationCapabilities =
        Capabilities(messageRepository, messageEventRepository, returnValueRepository)

    @Produces
    @ApplicationScoped
    fun topicNotifier(): TopicNotifier = PgSubscriberTopicNotifier(pgSubscriber)

    @Produces
    @ApplicationScoped
    fun eventLoop(
        messageEventRepository: MessageEventRepository,
        scopeCapabilities: ScopeCapabilities,
        jsonbHelper: JsonbHelper,
    ): EventLoop = EventLoop(fluentJdbc, messageEventRepository, scopeCapabilities, jsonbHelper)

    @Produces
    @ApplicationScoped
    fun messageQueue(
        topicNotifier: TopicNotifier,
        structuredCooperationCapabilities: StructuredCooperationCapabilities,
        messageRepository: MessageRepository,
        eventLoop: EventLoop,
    ): PostgresMessageQueue =
        PostgresMessageQueue(
            topicNotifier,
            structuredCooperationCapabilities,
            messageRepository,
            eventLoop,
        )
}

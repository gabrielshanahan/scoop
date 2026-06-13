package io.github.gabrielshanahan.scoop.quarkus

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.JsonbHelper
import io.github.gabrielshanahan.scoop.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.coroutine.TransactionRunner
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
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces
import java.time.Duration
import javax.sql.DataSource
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.eclipse.microprofile.config.inject.ConfigProperty

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
    private val dataSource: DataSource,
    @ConfigProperty(name = "scoop.tick-interval-ms", defaultValue = "50")
    private val tickIntervalMs: Long,
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

    /**
     * Produces the per-step [TransactionRunner]. Under Quarkus this is JTA-backed so that scoop's
     * `message_event` writes and any `@Transactional` business code a step calls share the same
     * Narayana transaction on the same Agroal connection, making the step atomic.
     */
    @Produces
    @ApplicationScoped
    fun transactionRunner(): TransactionRunner = JtaTransactionRunner(dataSource)

    @Produces
    @ApplicationScoped
    fun eventLoop(
        messageEventRepository: MessageEventRepository,
        scopeCapabilities: ScopeCapabilities,
        jsonbHelper: JsonbHelper,
        transactionRunner: TransactionRunner,
    ): EventLoop =
        EventLoop(
            fluentJdbc,
            messageEventRepository,
            scopeCapabilities,
            jsonbHelper,
            transactionRunner,
        )

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
            Duration.ofMillis(tickIntervalMs),
        )

    /**
     * Closes [PostgresMessageQueue]'s internal sleep-handler subscription on bean destroy so its
     * periodic tick stops before Quarkus tears down the Agroal [DataSource][javax.sql.DataSource].
     * Without this disposer the sleep-handler ticker keeps polling Postgres through Quarkus
     * shutdown and produces "Error in when ticking" log spam every time the surrounding test or
     * application exits.
     */
    fun disposeMessageQueue(@Disposes messageQueue: PostgresMessageQueue) {
        messageQueue.close()
    }
}

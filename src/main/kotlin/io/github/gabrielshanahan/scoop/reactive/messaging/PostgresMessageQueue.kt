package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.SLEEP_TOPIC
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.SleepEventLoopStrategy
import io.github.gabrielshanahan.scoop.reactive.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.messaging.Subscription
import io.smallrye.mutiny.Uni
import io.vertx.core.json.JsonObject
import io.vertx.mutiny.sqlclient.SqlConnection
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger

/**
 * Interactions with the message queue implementation. Supports [launching messages][launch],
 * [fetching messages][fetch] and [subscribing handlers (sagas) to a topic][subscribe]. Callers are
 * responsible for transaction management (by supplying a suitable [SqlConnection]) and payload
 * serialization (by supplying the [JsonObject]).
 *
 * Also keeps track of all subscribed handlers, as a placeholder solution to the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology).
 *
 * Delegates to [MessageRepository], [StructuredCooperationCapabilities] and [EventLoop] for most of
 * the actual work.
 *
 * ## Reactive Implementation
 *
 * This reactive implementation uses Mutiny's `Uni` types and Vert.x's reactive SQL client for
 * non-blocking database operations. All message operations return `Uni<T>` to enable composition
 * with other reactive operations.
 */
@ApplicationScoped
class PostgresMessageQueue(
    private val pgSubscriber: PgSubscriber,
    private val structuredCooperationCapabilities: StructuredCooperationCapabilities,
    private val messageRepository: MessageRepository,
    private val eventLoop: EventLoop,
) : HandlerRegistry {

    private val topicsToCoroutines =
        ConcurrentHashMap.newKeySet<Pair<String, DistributedCoroutineIdentifier>>()

    init {
        subscribe(
            SLEEP_TOPIC,
            saga("sleep-handler", SleepEventLoopStrategy(OffsetDateTime.now())) {
                step("sleep") { _, _ -> }
            },
        )
    }

    fun fetch(connection: SqlConnection, messageId: UUID): Uni<Message?> =
        messageRepository.fetchMessage(connection, messageId).map { message ->
            message?.also { logger.info("Fetched message: id=${it.id}, topic=${it.topic}") }
        }

    fun launch(
        connection: SqlConnection,
        topic: String,
        payload: JsonObject,
        context: CooperationContext? = null,
    ): Uni<CooperationRoot> =
        structuredCooperationCapabilities.launchOnGlobalScope(connection, topic, payload, context)

    fun subscribe(topic: String, saga: DistributedCoroutine): Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to saga.identifier)

        channel.handler {
            eventLoop
                .tick(topic, saga)
                .subscribe()
                .with(
                    {},
                    { e ->
                        logger.error(
                            "Event loop for ${saga.identifier} failed when triggered from LISTEN handler",
                            e,
                        )
                    },
                )
        }

        val subscription =
            eventLoop.tickPeriodically(
                topic,
                saga,
                // TODO: config
                Duration.ofMillis(50),
            )

        return Subscription {
            topicsToCoroutines.remove(topic to saga.identifier)
            subscription.close()
            channel.handler(null) // Executes UNLISTEN
        }
    }

    override fun listenersByTopic(): Map<String, List<String>> =
        topicsToCoroutines
            .map { (topic, identifier) -> topic to identifier.name }
            .distinct()
            .groupBy({ it.first }, { it.second })

    companion object {
        private val logger = Logger.getLogger(PostgresMessageQueue::class.java)
    }
}

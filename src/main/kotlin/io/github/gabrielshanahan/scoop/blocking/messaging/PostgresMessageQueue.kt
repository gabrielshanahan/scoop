package io.github.gabrielshanahan.scoop.blocking.messaging

import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SLEEP_TOPIC
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SleepEventLoopStrategy
import io.github.gabrielshanahan.scoop.blocking.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.messaging.Subscription
import io.vertx.pgclient.pubsub.PgChannel
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger
import org.postgresql.util.PGobject

/**
 * Interactions with the message queue implementation. Supports [launching messages][launch],
 * [fetching messages][fetch] and [subscribing handlers (sagas) to a topic][subscribe]. Callers are
 * responsible for transaction management (by supplying a suitable [Connection]) and payload
 * serialization (by supplying the [PGobject] - see [JsonbHelper]).
 *
 * Also keeps track of all subscribed handlers, as a placeholder solution to the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology).
 *
 * Delegates to [MessageRepository], [StructuredCooperationCapabilities] and [EventLoop] for most of
 * the actual work.
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

    /** This is explained in [SLEEP_TOPIC]. */
    init {
        subscribe(
            SLEEP_TOPIC,
            saga("sleep-handler", SleepEventLoopStrategy(OffsetDateTime.now())) {
                step("sleep") { _, _ -> }
            },
        )
    }

    /** Fetches a message, given its [messageId]. */
    fun fetch(connection: Connection, messageId: UUID): Message? =
        messageRepository.fetchMessage(connection, messageId)?.also {
            logger.info("Fetched message: id=${it.id}, topic=${it.topic}")
        }

    /** Launches a message on the global scope, i.e., a top-level message. */
    fun launch(
        connection: Connection,
        topic: String,
        payload: PGobject,
        context: CooperationContext? = null,
    ): CooperationRoot =
        structuredCooperationCapabilities.launchOnGlobalScope(connection, topic, payload, context)

    /**
     * Subscribes a [saga] (see
     * [SagaBuilder][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SagaBuilder]) to a
     * [topic].
     *
     * This amounts to doing two things:
     * 1) start a periodic process that checks if any saga is ready to be resumed, i.e., its next
     *    step is ready to be executed. The technical term for this kind of periodic process is an
     *    'event loop', and a single iteration a 'tick'. See [EventLoop].
     * 2) set up a notification pipeline that will run a 'tick' whenever a message is published to a
     *    topic. This is done via Postgres
     *    [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html), and is a non-critical
     *    part of the implementation - the periodic process above will always run `tick` eventually.
     *    The [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) is done in a
     *    database trigger - search for `notify_message_insert`.
     *
     * Returns a [Subscription], which can be used to cancel the periodic ticking, and execute
     * [UNLISTEN](https://www.postgresql.org/docs/current/sql-unlisten.html).
     */
    fun subscribe(topic: String, saga: DistributedCoroutine): Subscription {
        val channel: PgChannel = pgSubscriber.channel(topic)
        topicsToCoroutines.add(topic to saga.identifier)

        channel.handler { eventLoop.tick(topic, saga) }

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

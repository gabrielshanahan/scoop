package io.github.gabrielshanahan.scoop.messaging

import io.github.gabrielshanahan.scoop.JsonbHelper
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.coroutine.PeriodicTick
import io.github.gabrielshanahan.scoop.coroutine.builder.SLEEP_TOPIC
import io.github.gabrielshanahan.scoop.coroutine.builder.SleepEventLoopStrategy
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.StructuredCooperationCapabilities
import java.sql.Connection
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import org.postgresql.util.PGobject
import org.slf4j.LoggerFactory

private const val DEFAULT_TICK_INTERVAL_MS = 50L

/** Default interval between event loop polling ticks (50 milliseconds). */
val DEFAULT_TICK_INTERVAL: Duration = Duration.ofMillis(DEFAULT_TICK_INTERVAL_MS)

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
class PostgresMessageQueue(
    private val topicNotifier: TopicNotifier,
    private val structuredCooperationCapabilities: StructuredCooperationCapabilities,
    private val messageRepository: MessageRepository,
    private val eventLoop: EventLoop,
    private val tickInterval: Duration = DEFAULT_TICK_INTERVAL,
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
            logger.debug("Fetched message: id=${it.id}, topic=${it.topic}")
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
     * [SagaBuilder][io.github.gabrielshanahan.scoop.coroutine.builder.SagaBuilder]) to a [topic].
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
     * ## Multiple Instances
     *
     * [instances] controls how many independent workers are spun up for this saga within this JVM.
     * Each worker gets its own serialised tick loop and its own
     * [DistributedCoroutineIdentifier.instance][io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier.instance]
     * UUID under a shared name, and workers compete for pending saga runs using Postgres `FOR
     * UPDATE SKIP LOCKED` — exactly the same mechanism that lets multiple *service* instances scale
     * horizontally.
     *
     * Parallelism in Scoop comes exclusively from adding instances: a single worker never runs
     * overlapping continuations, because scheduled ticks and LISTEN/NOTIFY-driven ticks are
     * funneled through the same single-thread executor (see
     * [PeriodicTick][io.github.gabrielshanahan.scoop.coroutine.PeriodicTick]). Want N-way
     * in-process throughput? Set `instances = N`.
     *
     * The default ([instances] = 1) preserves the previous behaviour of one worker per
     * subscription.
     *
     * Returns a [Subscription], which can be used to cancel the periodic ticking of all instances
     * and unsubscribe from notifications.
     *
     * @param topic The message topic this saga handles
     * @param saga The saga definition (its [steps][DistributedCoroutine.steps] and
     *   [event loop strategy][DistributedCoroutine.eventLoopStrategy] are shared across all
     *   instances; they must be safe to invoke concurrently)
     * @param instances How many independent workers to spin up (must be `>= 1`, default 1)
     */
    fun subscribe(topic: String, saga: DistributedCoroutine, instances: Int = 1): Subscription {
        require(instances >= 1) { "instances must be >= 1, was $instances" }

        logger.debug(
            "Subscribing saga '{}' to topic '{}' with {} instance(s)",
            saga.identifier.name,
            topic,
            instances,
        )

        // First worker reuses the identifier the caller built, so `instances = 1` is byte-for-byte
        // identical to the pre-fan-out behaviour. Additional workers get fresh UUIDs under the
        // same saga name and therefore show up as distinct instances in logs, stack traces, and
        // message_event rows — matching the semantics of running N service instances.
        val workerSagas = buildList {
            add(saga)
            repeat(instances - 1) {
                add(
                    DistributedCoroutine(
                        identifier = DistributedCoroutineIdentifier(saga.identifier.name),
                        steps = saga.steps,
                        eventLoopStrategy = saga.eventLoopStrategy,
                    )
                )
            }
        }

        val workerHandles =
            workerSagas.map { workerSaga ->
                val periodicTick = eventLoop.tickPeriodically(topic, workerSaga, tickInterval)
                // Route the NOTIFY callback through the worker's own single-thread executor so
                // that push-driven ticks never run concurrently with the scheduled tick — each
                // worker stays strictly serial, and parallelism comes only from `instances`.
                val notifierHandle = topicNotifier.onMessage(topic) { periodicTick.trigger() }
                topicsToCoroutines.add(topic to workerSaga.identifier)
                WorkerHandle(workerSaga.identifier, notifierHandle, periodicTick)
            }

        return Subscription {
            workerHandles.forEach { handle ->
                topicsToCoroutines.remove(topic to handle.identifier)
                handle.periodicTick.close()
                handle.notifierHandle.close()
            }
        }
    }

    private data class WorkerHandle(
        val identifier: DistributedCoroutineIdentifier,
        val notifierHandle: AutoCloseable,
        val periodicTick: PeriodicTick,
    )

    override fun listenersByTopic(): Map<String, List<String>> =
        topicsToCoroutines
            .map { (topic, identifier) -> topic to identifier.name }
            .distinct()
            .groupBy({ it.first }, { it.second })

    /**
     * Minimum number of database connections Scoop may hold concurrently for its registered
     * workers' event loops.
     *
     * Each subscribed worker (each saga identifier produced by [subscribe]) holds at most one
     * connection at a time — it runs one [tick][EventLoop.tick] transaction at a time, and step
     * invocations within a tick run sequentially. The total concurrent demand for Scoop's own
     * event-loop work is therefore equal to the total number of subscribed worker instances across
     * all topics (which includes the internal `sleep-handler` subscription).
     *
     * Consumers can use this in an integration test to assert that their configured connection pool
     * is large enough to let every worker make progress without blocking for a connection:
     * ```kotlin
     * @Test
     * fun `jdbc pool is sized for registered workers`() {
     *     assertTrue(configuredPoolMaxSize >= messageQueue.requiredConnectionCount)
     * }
     * ```
     *
     * This count covers Scoop's tick work only. If your step bodies acquire additional connections
     * (for side-effect queries outside the step's transaction), add that to your sizing budget.
     */
    val requiredConnectionCount: Int
        get() = topicsToCoroutines.size

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresMessageQueue::class.java)
    }
}

package io.github.gabrielshanahan.scoop

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.github.gabrielshanahan.scoop.coroutine.EventLoop
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContextModule
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.Capabilities
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.MessageEventRepository
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ReturnValueRepository
import io.github.gabrielshanahan.scoop.messaging.MessageRepository
import io.github.gabrielshanahan.scoop.messaging.NoOpTopicNotifier
import io.github.gabrielshanahan.scoop.messaging.PostgresMessageQueue
import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import javax.sql.DataSource
import org.codejargon.fluentjdbc.api.FluentJdbcBuilder

/**
 * Main entry point for creating and using Scoop — the Structured Cooperation library.
 *
 * Provides a factory method for wiring together all Scoop components from a [DataSource].
 *
 * ## Usage
 *
 * ```kotlin
 * val scoop = Scoop.create(dataSource)
 * // Use scoop.messageQueue to subscribe sagas and launch messages
 * // ...
 * scoop.close() // Shut down when done
 * ```
 */
class Scoop
private constructor(
    val messageQueue: PostgresMessageQueue,
    val capabilities:
        io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.StructuredCooperationCapabilities,
) : AutoCloseable {

    companion object {
        /**
         * Creates a fully-wired Scoop instance from a [DataSource].
         *
         * @param dataSource JDBC DataSource connected to PostgreSQL
         * @param objectMapper Jackson ObjectMapper (default includes Kotlin and
         *   CooperationContextModule)
         * @param topicNotifier Notification mechanism for topic messages (default: polling only)
         */
        fun create(
            dataSource: DataSource,
            objectMapper: ObjectMapper = defaultObjectMapper(),
            topicNotifier: TopicNotifier = NoOpTopicNotifier,
        ): Scoop {
            val fluentJdbc = FluentJdbcBuilder().connectionProvider(dataSource).build()
            val jsonbHelper = JsonbHelper(objectMapper)
            val messageRepository = MessageRepository(fluentJdbc)
            val messageEventRepository = MessageEventRepository(jsonbHelper, fluentJdbc)
            val returnValueRepository = ReturnValueRepository(fluentJdbc)
            val capabilities =
                Capabilities(messageRepository, messageEventRepository, returnValueRepository)
            val eventLoop = EventLoop(fluentJdbc, messageEventRepository, capabilities, jsonbHelper)
            val messageQueue =
                PostgresMessageQueue(topicNotifier, capabilities, messageRepository, eventLoop)

            return Scoop(messageQueue, capabilities)
        }

        /** Creates a default ObjectMapper with Kotlin and CooperationContext support. */
        fun defaultObjectMapper(): ObjectMapper =
            ObjectMapper().apply {
                registerModule(KotlinModule.Builder().build())
                registerModule(CooperationContextModule(this))
            }
    }

    override fun close() {
        // Resources managed by the caller (DataSource lifecycle)
    }
}

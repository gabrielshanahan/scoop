package io.github.gabrielshanahan.scoop.reactive.coroutine

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.reactive.messaging.HandlerRegistry
import io.github.gabrielshanahan.scoop.reactive.messaging.PostgresMessageQueue
import io.vertx.mutiny.sqlclient.Pool
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeEach

abstract class StructuredCooperationTest {
    @Inject protected lateinit var messageQueue: PostgresMessageQueue

    @Inject protected lateinit var structuredCooperationCapabilities: StructuredCooperationCapabilities

    @Inject protected lateinit var handlerRegistry: HandlerRegistry

    @Inject protected lateinit var objectMapper: ObjectMapper

    @Inject protected lateinit var pool: Pool

    protected val rootTopic = "root-topic"
    protected val childTopic = "child-topic"
    protected val grandchildTopic = "grandchild-topic"

    @BeforeEach
    fun cleanupDatabase() {
        pool.executeAndAwaitPreparedQuery("TRUNCATE TABLE message_event, message CASCADE")
    }
}

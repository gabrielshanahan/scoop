package io.github.gabrielshanahan.scoop.blocking.coroutine

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.blocking.JsonbHelper
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.blocking.messaging.HandlerRegistry
import io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue
import jakarta.inject.Inject
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.junit.jupiter.api.BeforeEach

abstract class StructuredCooperationTest {
    @Inject protected lateinit var messageQueue: PostgresMessageQueue

    @Inject protected lateinit var structuredCooperationCapabilities: StructuredCooperationCapabilities

    @Inject protected lateinit var handlerRegistry: HandlerRegistry

    @Inject protected lateinit var objectMapper: ObjectMapper

    @Inject protected lateinit var fluentJdbc: FluentJdbc
    @Inject protected lateinit var jsonbHelper: JsonbHelper

    protected val rootTopic = "root-topic"
    protected val childTopic = "child-topic"
    protected val grandchildTopic = "grandchild-topic"

    @BeforeEach
    fun cleanupDatabase() {
        fluentJdbc.query().update("TRUNCATE TABLE message_event, message CASCADE").run()
    }
}

package io.github.gabrielshanahan.scoop.coroutine

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.JsonbHelper
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.StructuredCooperationCapabilities
import io.github.gabrielshanahan.scoop.messaging.HandlerRegistry
import io.github.gabrielshanahan.scoop.messaging.PostgresMessageQueue
import jakarta.inject.Inject
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.junit.jupiter.api.BeforeEach

abstract class StructuredCooperationTest {
    @Inject protected lateinit var messageQueue: PostgresMessageQueue

    @Inject
    protected lateinit var structuredCooperationCapabilities: StructuredCooperationCapabilities

    @Inject protected lateinit var handlerRegistry: HandlerRegistry

    @Inject protected lateinit var objectMapper: ObjectMapper

    @Inject protected lateinit var fluentJdbc: FluentJdbc
    @Inject protected lateinit var jsonbHelper: JsonbHelper

    protected val rootTopic = "root-topic"
    protected val childTopic = "child-topic"
    protected val grandchildTopic = "grandchild-topic"

    @BeforeEach
    fun cleanupDatabase() {
        // Pause the always-on internal sleep-handler subscription's periodic tick before
        // TRUNCATE. Without the pause, TRUNCATE's AccessExclusiveLock deadlocks with the
        // tick's AccessShareLock from `SELECT FOR UPDATE SKIP LOCKED`. The pause flag is
        // checked by the scheduled-tick path, so new ticks won't start; sleeping slightly
        // longer than the tick interval lets any in-flight tick drain before TRUNCATE runs.
        messageQueue.pauseTicks()
        try {
            // 60 ms = tick interval (50 ms) + safety margin.
            Thread.sleep(60)
            fluentJdbc
                .query()
                .update("TRUNCATE TABLE message_event, message, return_value CASCADE")
                .run()
        } finally {
            messageQueue.resumeTicks()
        }
    }
}

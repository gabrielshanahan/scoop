package io.github.gabrielshanahan.scoop.messaging

import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Two *distinct* sagas subscribed to the *same* topic must both be woken by a single message's
 * NOTIFY — and promptly, well inside the reconcile safety-net interval.
 *
 * This guards the fan-out in [io.github.gabrielshanahan.scoop.quarkus.PgSubscriberTopicNotifier]: a
 * Vert.x `PgChannel` has a single handler slot, so without per-topic fan-out the second `subscribe`
 * would overwrite the first's NOTIFY handler. With reconciliation gated on notifications, the
 * overwritten saga would then only reconcile on the safety-net sweep — its `SEEN` (and thus this
 * test's second latch) would be delayed by tens of seconds instead of one NOTIFY hop.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiHandlerTopicNotifyTest : StructuredCooperationTest() {

    @Test
    fun `both sagas on one topic are notified promptly by a single message`() {
        val latchA = CountDownLatch(1)
        val latchB = CountDownLatch(1)

        val subA =
            messageQueue.subscribe(
                rootTopic,
                saga("handler-a", handlerRegistry.eventLoopStrategy()) {
                    step { _, _ -> latchA.countDown() }
                },
            )
        val subB =
            messageQueue.subscribe(
                rootTopic,
                saga("handler-b", handlerRegistry.eventLoopStrategy()) {
                    step { _, _ -> latchB.countDown() }
                },
            )

        try {
            val payload = jsonbHelper.toPGobject(mapOf("k" to "v"))
            fluentJdbc.transactional { connection ->
                messageQueue.launch(connection, rootTopic, payload)
            }

            // 5s is far below the 30s safety net but ample for a NOTIFY-driven reconcile + resume,
            // so
            // a pass here means both handlers were woken by the notification, not by the safety
            // net.
            assertTrue(
                latchA.await(5, TimeUnit.SECONDS),
                "handler-a should run within one NOTIFY hop",
            )
            assertTrue(
                latchB.await(5, TimeUnit.SECONDS),
                "handler-b should run within one NOTIFY hop",
            )
        } finally {
            subA.close()
            subB.close()
        }
    }
}

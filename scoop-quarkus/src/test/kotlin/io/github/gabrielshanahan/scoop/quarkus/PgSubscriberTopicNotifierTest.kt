package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Verifies that [PgSubscriberTopicNotifier] dispatches callbacks off the Vert.x event loop thread.
 *
 * PgSubscriber delivers notifications on the Vert.x IO thread, but consumers (like
 * [io.github.gabrielshanahan.scoop.coroutine.EventLoop.tick]) perform blocking JDBC operations that
 * are forbidden on event loop threads. The notifier must therefore offload callbacks to a different
 * thread.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgSubscriberTopicNotifierTest {

    @Inject lateinit var topicNotifier: TopicNotifier
    @Inject lateinit var dataSource: DataSource

    @Test
    fun `callback should not run on vert-x event loop thread`() {
        val callbackThread = AtomicReference<Thread>()
        val latch = CountDownLatch(1)
        val topic = "test_notifier_thread"

        val handle =
            topicNotifier.onMessage(topic) {
                callbackThread.set(Thread.currentThread())
                latch.countDown()
            }

        handle.use {
            // Give PgSubscriber time to register the LISTEN
            Thread.sleep(200)

            // Fire a pg_notify on the topic to trigger the callback (same as the DB trigger does)
            dataSource.connection.use { conn ->
                conn.prepareStatement("SELECT pg_notify(?, ?)").use { stmt ->
                    stmt.setString(1, topic)
                    stmt.setString(2, "test")
                    stmt.execute()
                }
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback was not invoked within timeout")

            val thread = callbackThread.get()
            assertFalse(
                thread.name.contains("vert.x-eventloop"),
                "Callback ran on Vert.x event loop thread '${thread.name}' — " +
                    "this will block the IO thread when consumers perform JDBC operations",
            )
        }
    }
}

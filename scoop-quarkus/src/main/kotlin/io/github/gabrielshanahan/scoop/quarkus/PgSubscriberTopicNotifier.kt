package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.vertx.pgclient.pubsub.PgSubscriber

/**
 * Implements [TopicNotifier] using Vert.x [PgSubscriber] for PostgreSQL LISTEN/NOTIFY.
 *
 * Callbacks are dispatched on virtual threads rather than the Vert.x event loop thread, because
 * consumers (e.g. [io.github.gabrielshanahan.scoop.coroutine.EventLoop.tick]) perform blocking JDBC
 * operations that are forbidden on event loop threads.
 */
class PgSubscriberTopicNotifier(private val pgSubscriber: PgSubscriber) : TopicNotifier {
    override fun onMessage(topic: String, callback: () -> Unit): AutoCloseable {
        val channel = pgSubscriber.channel(topic)
        channel.handler { Thread.startVirtualThread(callback) }
        return AutoCloseable { channel.handler(null) }
    }
}

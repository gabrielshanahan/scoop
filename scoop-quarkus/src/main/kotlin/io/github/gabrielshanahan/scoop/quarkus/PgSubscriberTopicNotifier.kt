package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.vertx.pgclient.pubsub.PgSubscriber

/** Implements [TopicNotifier] using Vert.x [PgSubscriber] for PostgreSQL LISTEN/NOTIFY. */
class PgSubscriberTopicNotifier(private val pgSubscriber: PgSubscriber) : TopicNotifier {
    override fun onMessage(topic: String, callback: () -> Unit): AutoCloseable {
        val channel = pgSubscriber.channel(topic)
        channel.handler { callback() }
        return AutoCloseable { channel.handler(null) }
    }
}

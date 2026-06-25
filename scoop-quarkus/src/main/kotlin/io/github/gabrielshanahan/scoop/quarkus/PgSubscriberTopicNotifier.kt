package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.vertx.pgclient.pubsub.PgSubscriber
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Implements [TopicNotifier] using Vert.x [PgSubscriber] for PostgreSQL LISTEN/NOTIFY.
 *
 * Callbacks are dispatched on virtual threads rather than the Vert.x event loop thread, because
 * consumers (e.g. [io.github.gabrielshanahan.scoop.coroutine.EventLoop.tick]) perform blocking JDBC
 * operations that are forbidden on event loop threads.
 *
 * ## Fan-out
 *
 * A Vert.x [io.vertx.pgclient.pubsub.PgChannel] has a *single* handler slot, and
 * `pgSubscriber.channel(name)` returns the same channel for a given name. Registering one Vert.x
 * handler per [onMessage] call would therefore let the last subscriber to a topic silently
 * overwrite the handlers of all earlier subscribers — so only one of several workers/sagas on the
 * same topic (in the same JVM) would ever be notified. To avoid depending on that behaviour, this
 * notifier owns the fan-out: it registers exactly one Vert.x handler per topic and dispatches each
 * notification to every callback currently registered for that topic.
 */
class PgSubscriberTopicNotifier(private val pgSubscriber: PgSubscriber) : TopicNotifier {

    private val callbacksByTopic = ConcurrentHashMap<String, CopyOnWriteArrayList<() -> Unit>>()

    override fun onMessage(topic: String, callback: () -> Unit): AutoCloseable {
        val callbacks =
            callbacksByTopic.computeIfAbsent(topic) { topicName ->
                val list = CopyOnWriteArrayList<() -> Unit>()
                // One Vert.x handler per topic, dispatching to every registered callback. Each
                // callback runs on its own virtual thread (off the Vert.x event loop, see class
                // doc).
                pgSubscriber.channel(topicName).handler {
                    list.forEach { cb -> Thread.startVirtualThread(cb) }
                }
                list
            }
        callbacks.add(callback)

        return AutoCloseable {
            // Removing the callback is enough to stop it firing. The per-topic Vert.x handler is
            // left
            // in place even when the list empties: nulling it would race a concurrent re-subscribe
            // on
            // the shared PgChannel handler slot, and an empty list dispatches to nobody — a
            // harmless
            // no-op. PgSubscriber is application-scoped, so the residual idle handler lives only as
            // long as the app.
            callbacks.remove(callback)
        }
    }
}

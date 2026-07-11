package io.github.gabrielshanahan.scoop.quarkus

import io.github.gabrielshanahan.scoop.messaging.NotificationRegistration
import io.github.gabrielshanahan.scoop.messaging.TopicNotifier
import io.vertx.pgclient.pubsub.PgSubscriber
import java.util.concurrent.CompletableFuture
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
 *
 * ## Readiness
 *
 * Setting the channel handler only *initiates* the LISTEN — the registration completes
 * asynchronously, and a NOTIFY fired in that window is lost. The per-topic
 * [NotificationRegistration.ready] future completes when the channel's `subscribeHandler` fires
 * (the LISTEN is acknowledged), letting callers publish immediately after subscribing without
 * racing the registration.
 */
class PgSubscriberTopicNotifier(private val pgSubscriber: PgSubscriber) : TopicNotifier {

    private class TopicState {
        val callbacks = CopyOnWriteArrayList<() -> Unit>()
        val subscribed = CompletableFuture<Void>()
    }

    private val stateByTopic = ConcurrentHashMap<String, TopicState>()

    override fun onMessage(topic: String, callback: () -> Unit): NotificationRegistration {
        val state =
            stateByTopic.computeIfAbsent(topic) { topicName ->
                val topicState = TopicState()
                val channel = pgSubscriber.channel(topicName)
                // subscribeHandler must be wired before handler(), which initiates the LISTEN.
                // It also re-fires after reconnects; completing a completed future is a no-op.
                channel.subscribeHandler { topicState.subscribed.complete(null) }
                // One Vert.x handler per topic, dispatching to every registered callback. Each
                // callback runs on its own virtual thread (off the Vert.x event loop, see class
                // doc).
                channel.handler {
                    topicState.callbacks.forEach { cb -> Thread.startVirtualThread(cb) }
                }
                topicState
            }
        state.callbacks.add(callback)

        return object : NotificationRegistration {
            override val ready: CompletableFuture<Void>
                get() = state.subscribed

            override fun close() {
                // Removing the callback is enough to stop it firing. The per-topic Vert.x
                // handler is left in place even when the list empties: nulling it would race a
                // concurrent re-subscribe on the shared PgChannel handler slot, and an empty
                // list dispatches to nobody — a harmless no-op. PgSubscriber is
                // application-scoped, so the residual idle handler lives only as long as the
                // app.
                state.callbacks.remove(callback)
            }
        }
    }
}

package io.github.gabrielshanahan.scoop.messaging

import java.util.concurrent.CompletableFuture

/**
 * Handle for a registered notification callback.
 *
 * [close] unsubscribes the callback. [ready] completes once the underlying notification channel is
 * actually established (e.g. the PostgreSQL LISTEN registration has been acknowledged) — until
 * then, notifications published to the topic can be lost. Implementations with no asynchronous
 * registration step return an already-completed future.
 */
interface NotificationRegistration : AutoCloseable {
    val ready: CompletableFuture<Void>
}

/**
 * Abstraction for receiving notifications when messages are published to a topic.
 *
 * Implementations typically use database-specific features like PostgreSQL LISTEN/NOTIFY to provide
 * real-time message arrival notifications. The notifications are used as an optimization to
 * supplement polling — the system works correctly with just polling via the event loop.
 *
 * Registration is typically asynchronous (a LISTEN round-trip): a notification fired after
 * [onMessage] returns but before the registration completes is silently lost, and the subscriber
 * then waits for the reconcile safety net. Callers that publish immediately after subscribing
 * should await [NotificationRegistration.ready] (see [PostgresMessageQueue.subscribe], which does
 * this with a bounded wait).
 */
fun interface TopicNotifier {
    /** Register callback for topic notifications. Return handle to unsubscribe. */
    fun onMessage(topic: String, callback: () -> Unit): NotificationRegistration
}

/** No-op implementation that relies solely on polling. */
object NoOpTopicNotifier : TopicNotifier {
    override fun onMessage(topic: String, callback: () -> Unit) =
        object : NotificationRegistration {
            override val ready: CompletableFuture<Void> = CompletableFuture.completedFuture(null)

            override fun close() = Unit
        }
}

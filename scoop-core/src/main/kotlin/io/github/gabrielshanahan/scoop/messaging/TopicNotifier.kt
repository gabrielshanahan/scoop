package io.github.gabrielshanahan.scoop.messaging

/**
 * Abstraction for receiving notifications when messages are published to a topic.
 *
 * Implementations typically use database-specific features like PostgreSQL LISTEN/NOTIFY to provide
 * real-time message arrival notifications. The notifications are used as an optimization to
 * supplement polling — the system works correctly with just polling via the event loop.
 */
fun interface TopicNotifier {
    /** Register callback for topic notifications. Return handle to unsubscribe. */
    fun onMessage(topic: String, callback: () -> Unit): AutoCloseable
}

/** No-op implementation that relies solely on polling. */
object NoOpTopicNotifier : TopicNotifier {
    override fun onMessage(topic: String, callback: () -> Unit) = AutoCloseable {}
}

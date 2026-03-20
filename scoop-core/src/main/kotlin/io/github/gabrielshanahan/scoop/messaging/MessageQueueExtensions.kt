package io.github.gabrielshanahan.scoop.messaging

import io.github.gabrielshanahan.scoop.coroutine.Handler
import io.github.gabrielshanahan.scoop.coroutine.serializedValue

/**
 * Subscribes the given handler to its topic.
 *
 * This is a convenience method that extracts the topic and implementation from the handler and
 * delegates to the existing subscribe method.
 *
 * @param handler The handler to subscribe
 * @return The subscription for cleanup (UNLISTEN and cancel periodic ticking)
 *
 * Example:
 * ```kotlin
 * messageQueue.subscribe(OrderProcessor)
 * messageQueue.subscribe(PaymentProcessor)
 * ```
 */
fun PostgresMessageQueue.subscribe(handler: Handler<*>): Subscription =
    subscribe(handler.topic.serializedValue, handler.implementation())

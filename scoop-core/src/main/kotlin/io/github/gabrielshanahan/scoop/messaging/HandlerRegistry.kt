package io.github.gabrielshanahan.scoop.messaging

import io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.StandardEventLoopStrategy
import java.time.OffsetDateTime

/**
 * HandlerRegistry - Solves the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology)
 *
 * The short version is that structured cooperation requires knowing which handlers exist for each
 * topic to determine when all message emissions have corresponding handler starts. This interface
 * provides the handler topology information needed to enforce the core synchronization rule.
 *
 * @see StandardEventLoopStrategy Uses this registry to enforce structured cooperation
 */
interface HandlerRegistry {
    /**
     * Returns mapping of topics to handler names that listen to those topics.
     *
     * This information is used by
     * [EventLoopStrategy][io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.EventLoopStrategy]
     * to determine when all handlers for emitted messages have started execution, enabling
     * structured cooperation synchronization.
     *
     * @return Map where keys are topic names and values are lists of handler names
     */
    fun listenersByTopic(): Map<String, List<String>>
}

/**
 * Creates a [StandardEventLoopStrategy] using this registry's listener topology.
 *
 * [ignoreOlderThan] anchors the "ignore hierarchies older than" cutoff. It defaults to the client
 * clock at the time of the call, but the timestamps it is compared against
 * (`message_event.created_at`) come from the DATABASE clock — if the client clock runs even a few
 * milliseconds ahead of the database clock, a message launched right after this call is "older
 * than" the cutoff and is ignored forever (its SEEN is never created). When that skew matters, pass
 * a database-anchored timestamp (`SELECT clock_timestamp()`) instead.
 */
fun HandlerRegistry.eventLoopStrategy(ignoreOlderThan: OffsetDateTime = OffsetDateTime.now()) =
    StandardEventLoopStrategy(ignoreOlderThan, this::listenersByTopic)

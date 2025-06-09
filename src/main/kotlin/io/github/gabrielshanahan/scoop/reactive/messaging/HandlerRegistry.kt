package io.github.gabrielshanahan.scoop.reactive.messaging

import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.StandardEventLoopStrategy
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
     * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
     * to determine when all handlers for emitted messages have started execution, enabling
     * structured cooperation synchronization.
     *
     * @return Map where keys are topic names and values are lists of handler names
     */
    fun listenersByTopic(): Map<String, List<String>>
}

fun HandlerRegistry.eventLoopStrategy() =
    StandardEventLoopStrategy(OffsetDateTime.now(), this::listenersByTopic)

package io.github.gabrielshanahan.scoop.coroutine.builder

import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.Handler
import io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.EventLoopStrategy
import io.github.gabrielshanahan.scoop.coroutine.handlerName

/**
 * Builds a saga for this handler using its class name as the saga name.
 *
 * This extension delegates to the standard saga() builder function, passing this.handlerName as the
 * name parameter.
 *
 * Example:
 * ```kotlin
 * object OrderProcessor : Handler<OrderRequest>(Orders) {
 *     override fun implementation() = saga(eventLoopStrategy) {
 *         step("process") { scope, message -> /* ... */ }
 *     }
 * }
 * ```
 *
 * @param eventLoopStrategy Strategy that determines various aspects of when and how this saga will
 *   execute
 * @param block Configuration block that defines the saga's steps
 * @return A [DistributedCoroutine] ready to be subscribed to a message topic
 */
fun Handler<*>.saga(
    eventLoopStrategy: EventLoopStrategy,
    block: SagaBuilder.() -> Unit,
): DistributedCoroutine = saga(this.handlerName, eventLoopStrategy, block)

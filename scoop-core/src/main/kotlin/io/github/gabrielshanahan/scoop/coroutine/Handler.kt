package io.github.gabrielshanahan.scoop.coroutine

import io.github.gabrielshanahan.scoop.coroutine.builder.saga

/**
 * A type-safe identifier that binds together a handler name, its topic, and its implementation.
 *
 * Each handler defines its own singleton object extending this class, passing the topic (as a Topic
 * object) to the constructor and implementing the [implementation] method to provide the saga
 * logic. The handler's class name serves as the handler name.
 *
 * This provides compile-time safety for handler configuration and ensures handlers are always
 * associated with their topic and implementation.
 *
 * The type parameter [P] is propagated from the topic and represents the payload type that this
 * handler processes.
 *
 * Example:
 * ```kotlin
 * object OrderProcessor : Handler<OrderRequest>(Orders) {
 *     override fun implementation() = saga(eventLoopStrategy) {
 *         step("process") { scope, message -> /* ... */ }
 *     }
 * }
 * ```
 */
abstract class Handler<P>(val topic: Topic<P>) {
    /**
     * Returns the distributed coroutine implementation for this handler.
     *
     * The EventLoopStrategy is an internal implementation detail - handlers create or obtain the
     * strategy themselves rather than receiving it as a parameter. Typically implemented using the
     * [saga] extension method.
     */
    abstract fun implementation(): DistributedCoroutine
}

/**
 * The handler name used for database persistence and saga identification. Uses the simple class
 * name of the singleton object.
 */
val Handler<*>.handlerName: String
    get() = this::class.simpleName ?: error("Handler must be a named class (not anonymous)")

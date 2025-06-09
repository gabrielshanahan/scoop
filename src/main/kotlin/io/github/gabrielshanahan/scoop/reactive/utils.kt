package io.github.gabrielshanahan.scoop.reactive

import io.smallrye.mutiny.Uni

/**
 * Conditionally flat maps a Uni based on null value handling.
 *
 * This extension function provides null-safe flat mapping for reactive streams. If the upstream
 * value is null, it propagates a null item downstream without invoking the mapper function. This
 * prevents null pointer exceptions and maintains reactive stream semantics.
 *
 * ## Usage Patterns
 * Commonly used in saga step execution where optional values might be emitted:
 * ```kotlin
 * messageUni.flatMapNonNull { message ->
 *     processMessage(message)
 * }
 * ```
 *
 * @param mapper Function to transform non-null values into a new Uni
 * @return Uni that either contains the mapped result or propagates null
 */
fun <T, R> Uni<T>.flatMapNonNull(mapper: (T) -> Uni<R>): Uni<R> = flatMap {
    if (it == null) {
        Uni.createFrom().nullItem()
    } else {
        mapper(it)
    }
}

/**
 * Conditionally maps a Uni value, handling null cases gracefully.
 *
 * This extension function provides null-safe mapping for reactive streams. Uses Kotlin's safe call
 * operator to only apply the mapper function to non-null values, returning null for null inputs.
 *
 * ## Usage Patterns
 * Useful for transforming optional saga context values:
 * ```kotlin
 * contextUni.mapNonNull { context ->
 *     context.extractValue()
 * }
 * ```
 *
 * @param mapper Function to transform non-null values
 * @return Uni containing the mapped result or null
 */
fun <T, R> Uni<T>.mapNonNull(mapper: (T) -> R): Uni<R> = map { it?.let(mapper) }

/**
 * Lifts a synchronous binary function into the reactive Uni context.
 *
 * This utility function converts a regular function that operates on two parameters into a function
 * that returns a Uni. This is useful for integrating synchronous operations into reactive pipelines
 * without breaking the chain.
 *
 * ## Usage Patterns
 * Commonly used when combining saga results with blocking operations:
 * ```kotlin
 * val combinedOperation = unify { messageA: JsonObject, messageB: JsonObject ->
 *     messageA.mergeIn(messageB)
 * }
 * ```
 *
 * @param f Synchronous function to lift into reactive context
 * @return Function that applies f and wraps result in Uni
 */
fun <T, R, S> unify(f: (T, R) -> S): (T, R) -> Uni<S> = { t, r -> Uni.createFrom().item(f(t, r)) }

/**
 * Lifts a synchronous ternary function into the reactive Uni context.
 *
 * This utility function converts a regular function that operates on three parameters into a
 * function that returns a Uni. This overload handles cases where multiple saga results need to be
 * combined using synchronous operations.
 *
 * ## Usage Patterns
 * Useful for complex saga step coordination involving multiple inputs:
 * ```kotlin
 * val tripleOperation = unify { a: String, b: Int, c: Boolean ->
 *     BusinessLogic.process(a, b, c)
 * }
 * ```
 *
 * @param f Synchronous function to lift into reactive context
 * @return Function that applies f and wraps result in Uni
 */
fun <T, R, S, U> unify(f: (T, R, S) -> U): (T, R, S) -> Uni<U> = { t, r, s ->
    Uni.createFrom().item(f(t, r, s))
}

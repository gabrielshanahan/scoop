package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.annotation.JsonIgnore
import java.lang.reflect.ParameterizedType

/**
 * A map-like container for sharing contextual data across saga executions and cooperation lineages.
 * Heavily inspired by [kotlin.coroutines.CoroutineContext].
 *
 * [CooperationContext] is similar to reactive context in reactive streams or CoroutineContext in
 * Kotlin coroutines. It allows data to flow through the cooperation lineage - from parent sagas to
 * child sagas and between steps within the same saga.
 *
 * ## Lazy Deserialization Performance Design
 *
 * **The entire implementation is built around lazy deserialization** - all context data is stored
 * as JSON strings and only deserialized when first accessed via `get()`. As a consequence:
 * - Minimal cost unless context is actually used in a saga
 * - Doesn't try to deserialize contexts that are defined by different services
 *
 * Once accessed, deserialized values are cached to avoid repeated deserialization.
 *
 * The distinction between [MappedKey] and [UnmappedKey] addresses the fact that the context can
 * contain data that is just relevant for some services, and completely unknown to other:
 * - **MappedKey**: For context elements known to this service (deserialized to typed objects)
 * - **UnmappedKey**: For context elements from other services we don't know about (kept as JSON)
 *
 * This design enables cooperation across services with different codebases - each service can add
 * its own context elements while preserving unknown elements from other services.
 *
 * ## Built-in Context Types
 *
 * Scoop provides several built-in context elements:
 * - **Deadlines**: [AbsoluteDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.AbsoluteDeadline],
 *   [HappyPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.HappyPathDeadline],
 *   [RollbackPathDeadline][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.deadline.RollbackPathDeadline]
 *   for timeouts
 * - **Sleep**: See
 *   [SleepUntil][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SleepUntil]
 *
 * ## Context Inheritance
 *
 * Context flows through the cooperation lineage:
 * 1. When a message is emitted with additional context, it's merged with the current context
 * 2. Child handlers receive the combined context
 * 3. Context modifications in children don't affect parent sagas
 * 4. Context persists across saga suspensions and resumptions
 *
 * For more information, see
 * https://developer.porn/posts/implementing-structured-cooperation/#cooperationcontext
 */
interface CooperationContext {

    /**
     * Retrieves an element from the context by its key.
     *
     * @param key The key identifying the type of element to retrieve
     * @return The element if present, or null if not found
     */
    operator fun <E : Element> get(key: Key<E>): E?

    /**
     * Combines this context with another context, with the other context taking precedence.
     *
     * When both contexts contain elements with the same key, the combining behavior depends on the
     * implementation. The default behavior keeps the element from [context], but elements can
     * override [plus] to implement custom combining logic.
     *
     * ## Custom Combining Examples
     *
     * In [CooperationContextMap] (the most common implementation), when combining contexts that
     * both contain the same key, this method calls `element1.plus(element2)`, allowing elements to
     * define custom combining behavior:
     * - **[TryFinallyElement][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.TryFinallyElement]**:
     *   Merges lists of executed finally blocks
     * - **[CancellationToken]**: Uses logical AND to combine cancellation conditions
     * - **Default behavior**: Right-hand side takes precedence
     *
     * This is used to merge parent context with additional context when launching child operations.
     *
     * @param context The context to merge with this one
     * @return A new context containing elements from both contexts
     */
    operator fun plus(context: CooperationContext): CooperationContext

    /**
     * Creates a new context with the specified key removed.
     *
     * @param key The key of the element to remove
     * @return A new context without the specified element
     */
    operator fun minus(key: Key<*>): CooperationContext

    /**
     * Performs a fold operation over all elements in the context.
     *
     * This is useful for iterating over all context elements or converting the context to other
     * data structures.
     *
     * @param initial The initial value for the fold operation
     * @param operation Function called for each element in the context
     * @return The result of folding over all elements
     */
    fun <R> fold(initial: R, operation: (R, Element) -> R): R

    /**
     * A key that identifies a specific type of context element.
     *
     * Keys are used to store and retrieve context elements in a type-safe manner. The key type
     * reflects whether this service knows about the context element:
     * - [MappedKey]: Context elements defined by this service (typed deserialization)
     * - [UnmappedKey]: Context elements from other services this service doesn't know about
     *
     * This distinction enables distributed cooperation - when context flows between services, each
     * service can work with its own known elements while preserving unknown elements from other
     * services unchanged.
     *
     * The [simple name][Class.getSimpleName] of the key class is used for serialization.
     */
    sealed interface Key<E : Element>

    /**
     * A key for storing context data from other services that this service doesn't know about.
     *
     * When accessed, this key type wraps the raw JSON string in an [OpaqueElement] without parsing
     * it. This enables distributed cooperation across heterogeneous services:
     * - Preserves context elements added by other services with different codebases
     * - Allows this service to pass through unknown context data unchanged
     * - Provides maximum performance by avoiding unnecessary deserialization
     */
    data class UnmappedKey(val key: String) : Key<OpaqueElement>

    /**
     * A key for storing typed context elements that this service knows about.
     *
     * When accessed, this key type uses Jackson to deserialize the JSON string into a typed Kotlin
     * object that's defined in this service's codebase. This enables type-safe access to context
     * elements while supporting distributed cooperation.
     *
     * Implement this abstract class to create keys for your service's context elements. The class
     * uses reflection to determine the element type for deserialization.
     *
     * Example:
     * ```kotlin
     * object MyCustomDataKey : CooperationContext.MappedKey<MyCustomData>()
     * class MyCustomData(val value: String) : CooperationContext.MappedElement(MyCustomDataKey)
     * ```
     */
    abstract class MappedKey<E : MappedElement> : Key<E> {
        init {
            checkNotNull(mappedElementClass) {
                "${javaClass.name} must be parameterized with the element class."
            }
        }

        val mappedElementClass: Class<*>?
            @JsonIgnore
            get() {
                var currentClass: Class<*> = javaClass
                while (currentClass != Any::class.java) {
                    (currentClass.genericSuperclass as? ParameterizedType)?.let { paramType ->
                        return paramType.actualTypeArguments.firstOrNull()?.let {
                            when (it) {
                                is Class<*> -> it
                                is ParameterizedType -> it.rawType as? Class<*>
                                else -> null
                            }
                        }
                    }
                    currentClass = currentClass.superclass
                }
                return null
            }
    }

    /**
     * A single piece of data stored in the cooperation context.
     *
     * Each element is itself a valid [CooperationContext] containing only that element. This design
     * allows elements to be combined easily using the `+` operator.
     *
     * Elements are the atomic units of context data - they represent individual pieces of
     * information like deadlines, cancellation tokens, or other custom data.
     */
    sealed interface Element : CooperationContext {
        @get:JsonIgnore val key: Key<*>

        override operator fun <E : Element> get(key: Key<E>): E? =
            @Suppress("UNCHECKED_CAST") if (this.key == key) this as E else null

        override fun plus(context: CooperationContext): CooperationContext =
            if (context.has(key)) {
                context
            } else {
                CooperationContextMap(mutableMapOf(), mutableMapOf(key to this)) + context
            }

        override fun minus(key: Key<*>): CooperationContext =
            if (this.key == key) {
                emptyContext()
            } else {
                this
            }

        override fun <R> fold(initial: R, operation: (R, Element) -> R): R =
            operation(initial, this)
    }

    /**
     * An element that wraps JSON data from other services as a string.
     *
     * This element type stores raw JSON from context elements that this service doesn't recognize,
     * enabling distributed cooperation across heterogeneous services. The JSON remains unparsed,
     * allowing it to be passed through unchanged to other services that may understand it.
     */
    data class OpaqueElement(override val key: UnmappedKey, internal val json: String) : Element

    /**
     * Base class for typed context elements defined by this service.
     *
     * Extend this class to create context elements that this service understands and can work with
     * in a type-safe manner. The framework handles serialization and lazy deserialization
     * automatically, enabling the element to flow through distributed cooperation hierarchies.
     *
     * When context flows between services:
     * - Services that know about this element can deserialize and use it
     * - Services that don't know about it preserve it as [OpaqueElement]
     *
     * Example:
     * ```kotlin
     * object MyCustomDataKey : CooperationContext.MappedKey<MyCustomData>()
     *
     * data class MyCustomData(val userId: String, val sessionId: String)
     *     : CooperationContext.MappedElement(MyCustomDataKey)
     * ```
     */
    abstract class MappedElement(override val key: MappedKey<*>) : Element
}

fun CooperationContext.has(key: CooperationContext.Key<*>) = get(key) != null

val CooperationContext.size
    get() = fold(0) { acc, _ -> acc + 1 }

fun CooperationContext.isNotEmpty() = size > 0

val CooperationContext.Key<*>.serializedValue: String
    get() =
        when (this) {
            is CooperationContext.UnmappedKey -> key
            is CooperationContext.MappedKey<*> -> this::class.simpleName!!
        }

fun emptyContext(): CooperationContext = CooperationContextMap(mutableMapOf(), mutableMapOf())

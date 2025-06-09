package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.annotation.JsonIgnore
import java.lang.reflect.ParameterizedType

/**
 * A map-like container for sharing contextual data across saga executions and cooperation lineages.
 * 
 * [CooperationContext] is similar to reactive context in reactive streams or CoroutineContext in
 * Kotlin coroutines. It allows data to flow through the cooperation lineage - from parent sagas
 * to child sagas and between steps within the same saga.
 * 
 * ## Built-in Context Types
 * 
 * Scoop provides several built-in context elements:
 * - **Deadlines**: [AbsoluteDeadline], [HappyPathDeadline], [RollbackPathDeadline] for timeouts
 * - **Cancellation**: [CancellationToken] for cooperative cancellation
 * - **Custom data**: Any [MappedElement] for application-specific context
 * 
 * ## Context Inheritance
 * 
 * Context flows through the cooperation lineage:
 * 1. When a message is emitted with additional context, it's merged with the current context
 * 2. Child handlers receive the combined context
 * 3. Context modifications in children don't affect parent sagas
 * 4. Context persists across saga suspensions and resumptions
 * 
 * ## Usage Examples
 * 
 * ```kotlin
 * // Set a deadline for child operations
 * scope.context = scope.context + AbsoluteDeadline(Instant.now().plusSeconds(30))
 * 
 * // Launch with additional context
 * scope.launch("process-payment", payload, additionalContext = 
 *     emptyContext() + MyCustomData("important-info"))
 * 
 * // Check for cancellation
 * val cancellationToken = scope.context[CancellationToken]
 * if (cancellationToken?.isCancelled == true) {
 *     throw CancellationException()
 * }
 * ```
 * 
 * For more details on context propagation in distributed systems, see:
 * https://developer.porn/posts/implementing-structured-cooperation/
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
     * If both contexts contain elements with the same key, the element from [context] is kept.
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
     * This is useful for iterating over all context elements or converting the context
     * to other data structures.
     * 
     * @param initial The initial value for the fold operation
     * @param operation Function called for each element in the context
     * @return The result of folding over all elements
     */
    fun <R> fold(initial: R, operation: (R, Element) -> R): R

    /**
     * A key that identifies a specific type of context element.
     * 
     * Keys are used to store and retrieve context elements in a type-safe manner.
     * There are two types of keys:
     * - [UnmappedKey]: For storing arbitrary JSON data as strings
     * - [MappedKey]: For storing typed Kotlin objects
     * 
     * TODO: Doc that the SIMPLE name is used
     */
    sealed interface Key<E : Element>

    /**
     * A key for storing opaque JSON data in the context.
     * 
     * Use this when you need to store arbitrary data that doesn't have a specific
     * Kotlin type, or when you're working with dynamic/external data.
     */
    data class UnmappedKey(val key: String) : Key<OpaqueElement>

    /**
     * A key for storing typed Kotlin objects in the context.
     * 
     * Implement this abstract class to create keys for your custom context elements.
     * The class uses reflection to determine the element type for serialization.
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
     * Each element is itself a valid [CooperationContext] containing only that element.
     * This design allows elements to be combined easily using the `+` operator.
     * 
     * Elements are the atomic units of context data - they represent individual pieces
     * of information like deadlines, cancellation tokens, or custom application data.
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
     * An element that stores arbitrary JSON data.
     * 
     * This is used for storing untyped data in the context when you don't want to
     * create a specific Kotlin class for the data structure.
     */
    data class OpaqueElement(override val key: UnmappedKey, internal val json: String) : Element

    /**
     * Base class for typed context elements.
     * 
     * Extend this class to create custom context elements with specific Kotlin types.
     * The framework will handle serialization and deserialization automatically.
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

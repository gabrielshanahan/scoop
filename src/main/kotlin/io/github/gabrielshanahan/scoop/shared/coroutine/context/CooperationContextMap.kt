package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * CooperationContextMap - The primary implementation of lazy deserialization for
 * [CooperationContext]
 *
 * This is the core implementation that enables the distributed cooperation context system to work
 * efficiently across heterogeneous services. It maintains context elements in two forms:
 * - **Serialized form**: JSON strings that can flow between any services
 * - **Deserialized form**: Typed Kotlin objects that are only created when accessed
 *
 * ## Two-Map Architecture
 *
 * The implementation uses two internal maps to achieve lazy deserialization:
 * - **[serializedMap]**: All context data as JSON strings (key name → JSON string)
 * - **[deserializedMap]**: Cached deserialized objects (Key → Element)
 *
 * This dual representation allows:
 * - **Performance**: Only deserialize data when it's actually accessed
 * - **Interoperability**: Pass through unknown context from other services unchanged
 * - **Type safety**: Provide typed access to known context elements
 *
 * ## Lazy Deserialization Flow
 * 1. **Initial state**: All context starts in [serializedMap] as JSON strings
 * 2. **First access**: [get] method checks [deserializedMap] first (cache hit)
 * 3. **Cache miss**: Looks up JSON in [serializedMap] and deserializes based on key type:
 *     - [MappedKey]: Uses Jackson to deserialize to typed object
 *     - [UnmappedKey]: Wraps JSON string in [OpaqueElement]
 * 4. **Caching**: Stores deserialized result in [deserializedMap] for future access
 *
 * ## Custom Element Combining
 *
 * When contexts are combined with [plus], this implementation enables custom combining logic by
 * calling `element1.plus(element2)` when both contexts contain the same key. This allows elements
 * like
 * [TryFinallyElement][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.TryFinallyElement]
 * and [CancellationToken] to implement custom merging behaviors rather than just simple
 * replacement.
 *
 * @param serializedMap JSON strings keyed by element name (from
 *   [CooperationContextJacksonCustomizer])
 * @param deserializedMap Cached deserialized elements keyed by their [CooperationContext.Key]
 * @param objectMapper Jackson mapper for deserializing [MappedKey] elements (null in pure creation
 *   scenarios)
 */
data class CooperationContextMap(
    private val serializedMap: MutableMap<String, String>,
    private val deserializedMap: MutableMap<CooperationContext.Key<*>, CooperationContext.Element>,
    private val objectMapper: ObjectMapper? = null,
) : CooperationContext {

    /**
     * Retrieves a context element by key, deserializing lazily on first access.
     *
     * This method implements the core lazy deserialization strategy:
     * 1. Check [deserializedMap] first (cache hit - return immediately)
     * 2. If not cached and no [objectMapper], return null or cached value
     * 3. Look up JSON string in [serializedMap]
     * 4. Deserialize based on key type:
     *     - [MappedKey]: Use Jackson to deserialize to typed object
     *     - [UnmappedKey]: Wrap JSON in [OpaqueElement]
     * 5. Cache result in [deserializedMap] and return
     *
     * **Performance**: Only deserializes when needed, so minimal cost unless actually accessed.
     * This enables passing large contexts through services that don't use them.
     */
    @Suppress("UNCHECKED_CAST")
    override fun <E : CooperationContext.Element> get(key: CooperationContext.Key<E>): E? {
        if (deserializedMap[key] != null || objectMapper == null) {
            return (deserializedMap[key] as E?)
        }

        val serializedElement = serializedMap[key.serializedValue] ?: return null
        val deserializedElement =
            when (key) {
                is CooperationContext.UnmappedKey ->
                    CooperationContext.OpaqueElement(key, serializedElement)
                is CooperationContext.MappedKey<*> ->
                    objectMapper.readValue(serializedElement, key.mappedElementClass)
            }
        return (deserializedElement as E?)?.also { deserializedMap[key] = it }
    }

    /**
     * Combines this context with another, enabling custom element combining logic like
     * [TryFinallyElement][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.TryFinallyElement]
     * list merging and [CancellationToken] logical AND.
     *
     * **Combining Strategy**:
     * - **Single element**: If key exists, calls `existingElement.plus(newElement)`
     * - **Two CooperationContextMaps**: Forces deserialization of all keys in both maps, then
     *   applies element-level combining for conflicts
     * - **Other contexts**: Falls back to fold-based combining
     *
     * **Important limitation**: Custom plus logic only runs for keys that have been "touched"
     * (accessed) in either map. In practice this doesn't matter because:
     * - When we create maps ourselves, all keys are touched
     * - Untouched keys only exist when deserializing from messages
     * - Adding a map to itself has no effect anyway
     *
     * **Performance consideration**: Combining two [CooperationContextMap]s forces deserialization
     * of all elements to enable proper custom combining.
     */
    override fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is CooperationContext.Element ->
                if (has(context.key)) {
                    (this - context.key) + (get(context.key)!! + context)
                } else {
                    CooperationContextMap(
                        serializedMap,
                        buildMap {
                                putAll(deserializedMap)
                                put(context.key, context)
                            }
                            .toMutableMap(),
                        objectMapper,
                    )
                }
            is CooperationContextMap -> {
                val deserializedKeys = deserializedMap.keys + context.deserializedMap.keys

                // Make sure all keys deserialized in one map are also deserialized in the other
                // map, so we can run instance-specific logic when calling
                // CooperationContext.Element.plus
                deserializedKeys.forEach { key ->
                    get(key)
                    context[key]
                }

                CooperationContextMap(
                    serializedMap.toMutableMap().apply { putAll(context.serializedMap) },
                    deserializedKeys.associateWithTo(mutableMapOf()) { key ->
                        when {
                            key in deserializedMap && key in context.deserializedMap ->
                                (deserializedMap.getValue(key) +
                                    context.deserializedMap.getValue(key))[key]!!
                            key in deserializedMap -> deserializedMap.getValue(key)
                            else -> context.deserializedMap.getValue(key)
                        }
                    },
                    context.objectMapper ?: objectMapper,
                )
            }
            else -> context.fold(this, CooperationContext::plus)
        }

    /**
     * Creates a new context with the specified key removed from both maps.
     *
     * This removes the element from both the serialized and deserialized representations to
     * maintain consistency between the two maps.
     */
    override fun minus(key: CooperationContext.Key<*>): CooperationContext =
        if (!has(key)) {
            this
        } else {
            CooperationContextMap(
                serializedMap.toMutableMap().also { it.remove(key.serializedValue) },
                deserializedMap.toMutableMap().also { it.remove(key) },
                objectMapper,
            )
        }

    /**
     * Folds over all context elements, presenting a unified view of both maps.
     *
     * This method creates a logical union of both maps:
     * - Elements in [serializedMap] are presented as [OpaqueElement]s
     * - Elements in [deserializedMap] are presented as their typed objects
     * - Duplicates are handled by preferring deserialized elements
     *
     * Results are sorted by key name for deterministic iteration order.
     */
    override fun <R> fold(initial: R, operation: (R, CooperationContext.Element) -> R): R =
        buildSet {
                serializedMap.forEach { (key, value) ->
                    add(
                        CooperationContext.OpaqueElement(CooperationContext.UnmappedKey(key), value)
                    )
                }
                addAll(deserializedMap.values)
            }
            .toSortedSet(compareBy { it.key.serializedValue })
            .fold(initial, operation)
}

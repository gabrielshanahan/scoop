package io.github.gabrielshanahan.scoop.coroutine

/**
 * A type-safe identifier for a message topic/channel.
 *
 * Each topic is defined as a singleton object extending this class. The object itself is the
 * identifier - there is no string value to pass. Topics identify the message channel that handlers
 * subscribe to.
 *
 * The type parameter [P] represents the payload type that messages on this topic carry. This is
 * currently for documentation purposes only and is not enforced at runtime.
 *
 * Example:
 * ```kotlin
 * object Orders : Topic<OrderRequest>()
 * object Payments : Topic<PaymentRequest>()
 * ```
 */
abstract class Topic<P>

/**
 * The string representation used for database persistence. Uses the simple class name of the
 * singleton object.
 */
val Topic<*>.serializedValue: String
    get() = this::class.simpleName ?: error("Topic must be a named class (not anonymous)")

package io.github.gabrielshanahan.scoop.shared.coroutine

import com.github.f4b6a3.uuid.UuidCreator

/**
 * Identifies a distributed coroutine (saga) implementation, with support for horizontal scaling.
 *
 * This identifier distinguishes between different saga handlers in a distributed system where
 * multiple service instances may be running the same saga logic.
 *
 * ## Horizontal Scaling Support
 *
 * The [instance] field enables multiple instances of the same service to run independently:
 * - **name**: Identifies the saga type/logic (e.g., "order-processor", "payment-handler")
 * - **instance**: Identifies the specific service instance running that saga (UUID)
 *
 * For example, if you have 3 instances of an order processing service running:
 * ```
 * DistributedCoroutineIdentifier("order-processor", "uuid-instance-1")
 * DistributedCoroutineIdentifier("order-processor", "uuid-instance-2")
 * DistributedCoroutineIdentifier("order-processor", "uuid-instance-3")
 * ```
 *
 * The single-parameter constructor automatically generates a unique instance identifier using
 * time-ordered UUIDs.
 *
 * This identifier appears, among other places, in distributed stack traces and error messages,
 * helping identify which specific service instance encountered issues during saga execution.
 *
 * @param name The saga type/handler name (shared across all instances)
 * @param instance Unique identifier for the specific service instance
 */
data class DistributedCoroutineIdentifier(val name: String, val instance: String) {
    /**
     * Creates a distributed coroutine identifier with an auto-generated instance UUID.
     *
     * The instance identifier is generated using time-ordered UUIDs to ensure uniqueness across
     * horizontally scaled service instances.
     *
     * @param name The saga type/handler name
     */
    constructor(name: String) : this(name, UuidCreator.getTimeOrderedEpoch().toString())
}

/**
 * Renders the identifier in a human-readable format for logging and debugging.
 *
 * Format: "saga-name[instance-uuid]"
 *
 * This format appears in:
 * - Distributed exception stack traces
 * - Saga execution logs
 * - Error messages and debugging output
 *
 * @return String representation in the format "name[instance]"
 */
fun DistributedCoroutineIdentifier.renderAsString() = "$name[$instance]"

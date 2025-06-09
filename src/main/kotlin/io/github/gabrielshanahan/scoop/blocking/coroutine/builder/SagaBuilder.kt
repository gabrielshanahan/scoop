package io.github.gabrielshanahan.scoop.blocking.coroutine.builder

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy

/**
 * Fluent DSL builder for creating distributed sagas participating in structured cooperation.
 *
 * [SagaBuilder] provides a clean, readable way to define [DistributedCoroutine] instances, which is
 * how Scoop implements the saga pattern - as sequences of [TransactionalStep]s.
 *
 * ## Usage Pattern
 *
 * The builder is typically used through the [saga] function which provides the DSL entry point:
 * ```kotlin
 * val orderSaga = saga("order-processor", someEventLoopStrategyInstance) {
 *     step("validate-order") { scope, message ->
 *         val order = parseOrder(message.payload)
 *         validateOrderData(order)
 *         scope.launch("payment-topic", createPaymentRequest(order))
 *     }
 *
 *     step("confirm-order") { scope, message ->
 *         val orderId = extractOrderId(message.payload)
 *         confirmOrder(orderId)
 *         scope.launch("shipping-topic", createShippingRequest(orderId))
 *     }
 * }
 *
 * messageQueue.subscribe("order-topic", orderSaga)
 * ```
 *
 * In Scoop, a dummy implementation of [EventLoopStrategy] is provided by
 * [HandlerRegistry.eventLoopStrategy()][io.github.gabrielshanahan.scoop.blocking.messaging.eventLoopStrategy].
 * For more information, read about the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology).
 *
 * ## Step Definition Methods
 *
 * The builder provides several overloaded [step] methods for different use cases:
 * - **Basic step**: Just an invoke block for happy path logic
 * - **Step with rollback**: Includes compensating action for failure scenarios
 * - **Step with error handling**: Includes child failure handling logic
 * - **Named steps**: Explicit step names for better tracking and debugging
 *
 * ## Database Integration
 *
 * The saga name becomes the `coroutine_name` in the `message_event` table, enabling tracking and
 * debugging of saga executions across the distributed system.
 *
 * For more examples or information, see the test files and the blog posts:
 * - https://developer.porn/posts/introducing-structured-cooperation/
 * - https://developer.porn/posts/implementing-structured-cooperation/
 */
class SagaBuilder(val name: String, val eventLoopStrategy: EventLoopStrategy) {

    val steps: MutableList<TransactionalStep> = mutableListOf()

    fun step(
        name: String,
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) {
        steps.add(
            object : TransactionalStep {
                override val name: String
                    get() = name

                override fun invoke(scope: CooperationScope, message: Message) =
                    invoke(scope, message)

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) =
                    rollback?.invoke(scope, message, throwable)
                        ?: super.rollback(scope, message, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) =
                    handleChildFailures?.invoke(scope, message, throwable)
                        ?: super.handleChildFailures(scope, message, throwable)
            }
        )
    }

    fun step(
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) = step(steps.size.toString(), invoke, rollback, handleChildFailures)

    fun step(name: String, invoke: (CooperationScope, Message) -> Unit) =
        step(name, invoke, null, null)

    fun step(invoke: (CooperationScope, Message) -> Unit) =
        step(steps.size.toString(), invoke, null, null)

    fun build(): DistributedCoroutine =
        DistributedCoroutine(DistributedCoroutineIdentifier(name), steps, eventLoopStrategy)
}

/**
 * Creates a distributed saga using the builder DSL.
 *
 * This is the main entry point for defining sagas in Scoop. It creates a [SagaBuilder] instance,
 * applies the provided configuration lambda to it, and builds the resulting [DistributedCoroutine].
 *
 * ## Basic Usage
 *
 * ```kotlin
 * val mySaga = saga("handler-name", someEventLoopStrategyInstance) {
 *     step { scope, message ->
 *         // Process message and emit child messages
 *         scope.launch("child-topic", childMessage)
 *     }
 * }
 *
 * messageQueue.subscribe("input-topic", mySaga)
 * ```
 *
 * ## Advanced Usage with rollback and name
 *
 * ```kotlin
 * val transactionalSaga = saga("payment-processor", someEventLoopStrategyInstance) {
 *     step(
 *         name = "charge-card",
 *         invoke = { scope, message ->
 *             val chargeId = paymentService.chargeCard(message.payload)
 *             scope.launch("notify-customer", createNotification(chargeId))
 *         },
 *         rollback = { scope, message, throwable ->
 *             paymentService.refundCharge(extractChargeId(message.payload))
 *         }
 *     )
 * }
 * ```
 *
 * @param name Unique name for this saga type (used in message_event table)
 * @param eventLoopStrategy Strategy that determines various aspects of when and how this saga will
 *   execute
 * @param block Configuration block that defines the saga's steps
 * @return A [DistributedCoroutine] ready to be subscribed to a message topic
 */
fun saga(
    name: String,
    eventLoopStrategy: EventLoopStrategy,
    block: SagaBuilder.() -> Unit,
): DistributedCoroutine = SagaBuilder(name, eventLoopStrategy).apply(block).build()

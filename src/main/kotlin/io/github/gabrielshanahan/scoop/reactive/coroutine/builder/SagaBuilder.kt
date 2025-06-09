package io.github.gabrielshanahan.scoop.reactive.coroutine.builder

import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.reactive.unify
import io.github.gabrielshanahan.scoop.shared.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy
import io.smallrye.mutiny.Uni

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
 * [HandlerRegistry.eventLoopStrategy()][io.github.gabrielshanahan.scoop.reactive.messaging.eventLoopStrategy].
 * For more information, read about the
 * ["who is listening" problem](https://developer.porn/posts/implementing-structured-cooperation/#building-and-maintaining-a-handler-topology).
 *
 * ## Step Definition Methods
 *
 * The builder provides several overloaded methods for different use cases:
 * - **[step]**: Blocking step definitions (automatically converted to [Uni] internally)
 * - **[uniStep]**: Native reactive step definitions that return [Uni] directly
 * - **Named steps**: Explicit step names for better tracking and debugging
 * - **Step with rollback**: Includes compensating action for failure scenarios
 * - **Step with error handling**: Includes child failure handling logic
 *
 * ## Database Integration
 *
 * The saga name becomes the `coroutine_name` in the `message_event` table, enabling tracking and
 * debugging of saga executions across the distributed system.
 *
 * ## Reactive Implementation
 *
 * This reactive implementation provides both blocking-style [step] methods (for convenience) and
 * native reactive [uniStep] methods. The blocking methods are automatically converted to reactive
 * using the [unify] function.
 *
 * For more examples or information, see the test files and the blog posts:
 * - https://developer.porn/posts/introducing-structured-cooperation/
 * - https://developer.porn/posts/implementing-structured-cooperation/
 */
class SagaBuilder(val name: String, val eventLoopStrategy: EventLoopStrategy) {

    val steps: MutableList<TransactionalStep> = mutableListOf()

    fun uniStep(
        name: String,
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
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
                ): Uni<Unit> =
                    rollback?.invoke(scope, message, throwable)
                        ?: super.rollback(scope, message, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ): Uni<Unit> =
                    handleChildFailures?.invoke(scope, message, throwable)
                        ?: super.handleChildFailures(scope, message, throwable)
            }
        )
    }

    fun uniStep(
        invoke: (CooperationScope, Message) -> Uni<Unit>,
        rollback: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Uni<Unit>)? = null,
    ) = uniStep(steps.size.toString(), invoke, rollback, handleChildFailures)

    fun uniStep(invoke: (CooperationScope, Message) -> Uni<Unit>) =
        uniStep(steps.size.toString(), invoke, null, null)

    fun step(
        name: String,
        invoke: (CooperationScope, Message) -> Unit,
        rollback: ((CooperationScope, Message, Throwable) -> Unit)? = null,
        handleChildFailures: ((CooperationScope, Message, Throwable) -> Unit)? = null,
    ) = uniStep(name, unify(invoke), rollback?.let(::unify), handleChildFailures?.let(::unify))

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

fun saga(
    name: String,
    eventLoopStrategy: EventLoopStrategy,
    block: SagaBuilder.() -> Unit,
): DistributedCoroutine = SagaBuilder(name, eventLoopStrategy).apply(block).build()

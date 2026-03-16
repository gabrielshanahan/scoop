package io.github.gabrielshanahan.scoop.coroutine.builder

import io.github.gabrielshanahan.scoop.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.coroutine.NextStep
import io.github.gabrielshanahan.scoop.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.coroutine.eventloop.ChildFailureHandlerIteration
import io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.EventLoopStrategy
import io.github.gabrielshanahan.scoop.messaging.Message

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
 * [HandlerRegistry.eventLoopStrategy()][io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy].
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

    /**
     * Adds a step with simplified lambdas that use default behavior for advanced parameters. The
     * invoke lambda ignores [stepIteration][TransactionalStep.invoke] and always returns
     * [NextStep.Continue]. The rollback lambda ignores [stepIteration][TransactionalStep.rollback]
     * and [childFailureHandlerIteration][TransactionalStep.rollback]. The handleChildFailures
     * lambda ignores [childFailureHandlerIteration][TransactionalStep.handleChildFailures] and
     * [nextStep][TransactionalStep.handleChildFailures].
     */
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

                override fun invoke(
                    scope: CooperationScope,
                    message: Message,
                    stepIteration: Int,
                ): NextStep {
                    invoke(scope, message)
                    return NextStep.Continue
                }

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: ChildFailureHandlerIteration,
                ) =
                    rollback?.invoke(scope, message, throwable)
                        ?: super.rollback(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                        )

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: Int,
                    nextStep: NextStep,
                ): NextStep =
                    if (handleChildFailures != null) {
                        handleChildFailures.invoke(scope, message, throwable)
                        nextStep
                    } else {
                        super.handleChildFailures(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep,
                        )
                    }
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

    /**
     * Adds a stepIteration-aware step that can control loop execution via [NextStep].
     *
     * The invoke lambda receives the current stepIteration and returns [NextStep.Continue] to
     * advance to the next step, [NextStep.Repeat] to re-execute this step, or [NextStep.GoTo] to
     * jump to a specific step index.
     */
    fun step(
        name: String,
        invoke: (scope: CooperationScope, message: Message, stepIteration: Int) -> NextStep,
        rollback:
            ((
                scope: CooperationScope,
                message: Message,
                throwable: Throwable,
                stepIteration: Int,
                childFailureHandlerIteration: ChildFailureHandlerIteration,
            ) -> Unit)? =
            null,
        handleChildFailures:
            ((
                scope: CooperationScope,
                message: Message,
                throwable: Throwable,
                stepIteration: Int,
                childFailureHandlerIteration: Int,
                nextStep: NextStep,
            ) -> NextStep)? =
            null,
    ) {
        steps.add(
            object : TransactionalStep {
                override val name: String
                    get() = name

                override fun invoke(
                    scope: CooperationScope,
                    message: Message,
                    stepIteration: Int,
                ): NextStep = invoke(scope, message, stepIteration)

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: ChildFailureHandlerIteration,
                ) =
                    rollback?.invoke(
                        scope,
                        message,
                        throwable,
                        stepIteration,
                        childFailureHandlerIteration,
                    )
                        ?: super.rollback(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                        )

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: Int,
                    nextStep: NextStep,
                ): NextStep =
                    handleChildFailures?.invoke(
                        scope,
                        message,
                        throwable,
                        stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    )
                        ?: super.handleChildFailures(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep,
                        )
            }
        )
    }

    fun step(
        invoke: (scope: CooperationScope, message: Message, stepIteration: Int) -> NextStep,
        rollback:
            ((
                scope: CooperationScope,
                message: Message,
                throwable: Throwable,
                stepIteration: Int,
                childFailureHandlerIteration: ChildFailureHandlerIteration,
            ) -> Unit)? =
            null,
        handleChildFailures:
            ((
                scope: CooperationScope,
                message: Message,
                throwable: Throwable,
                stepIteration: Int,
                childFailureHandlerIteration: Int,
                nextStep: NextStep,
            ) -> NextStep)? =
            null,
    ) = step(steps.size.toString(), invoke, rollback, handleChildFailures)

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

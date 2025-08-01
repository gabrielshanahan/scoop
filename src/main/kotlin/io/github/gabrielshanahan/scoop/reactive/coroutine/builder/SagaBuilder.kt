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
 * Used to define sagas, i.e., instances of [DistributedCoroutine], which can then be subscribed to a topic
 * via [MessageQueue.subscribe][io.github.gabrielshanahan.scoop.reactive.messaging.PostgresMessageQueue.subscribe].
 * Fundamentally, it just gives you a nice interface to describe the individual [TransactionalSteps][TransactionalStep].
 *
 * [SagaBuilder] is not meant to be used directly, but rather facilitates a DSL that can be accessed via
 * the [saga] function. This function accepts a name, which will be used as part of the [DistributedCoroutine]
 * identifier (and is what is used in the `coroutine_name` column of `message_event`), and an [EventLoopStrategy].
 *
 * Example usage:
 *
 * ```
 * saga("some-handler", handlerRegistry.eventLoopStrategy()) {
 *     step(
 *         invoke = { scope, message ->
 *             // Do stuff
 *         },
 *         rollback = { scope, message, throwable ->
 *             // Rollback stuff
 *         },
 *     )
 *     step { scope, message ->
 *         // Do more stuff
 *     }
 * }
 * ```
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

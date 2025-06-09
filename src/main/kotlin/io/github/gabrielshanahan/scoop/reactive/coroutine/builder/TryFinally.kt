package io.github.gabrielshanahan.scoop.reactive.coroutine.builder

import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext

/**
 * Try-finally semantics for distributed sagas using CooperationContext tracking.
 *
 * This file provides try-finally-like resource management patterns that work correctly across
 * distributed saga boundaries. It ensures that "finally" blocks run exactly once, whether the saga
 * completes successfully or needs to roll back.
 *
 * ## How It Works
 *
 * The implementation uses [CooperationContext] to track which finally blocks have already been
 * executed. This prevents duplicate executions during complex rollback scenarios.
 *
 * ## Usage Pattern
 *
 * ```kotlin
 * saga("resource-handler") {
 *     tryFinallyStep(
 *         invoke = { scope, message ->
 *             resourceService.acquire("resource-123")
 *             scope.launch("do-work-topic", workMessage)
 *         },
 *         finally = { scope, message ->
 *             resourceService.release("resource-123")
 *         }
 *     )
 * }
 * ```
 *
 * ## Execution Guarantees
 * - **Success path**: Finally block runs in the second step after invoke completes
 * - **Failure path**: Finally block runs during rollback if invoke succeeded
 * - **Exception in invoke**: Finally block runs immediately before re-throwing
 * - **Exactly once**: Finally blocks never run more than once per saga execution
 *
 * ## Implementation Details
 *
 * The [tryFinallyStep] function creates two actual saga steps:
 * 1. **Try step**: Runs the invoke logic, with exception handling to run finally
 * 2. **Finally step**: Runs the finally logic if not already executed
 *
 * The [TryFinallyElement] in the context tracks which finally blocks have been executed.
 */

/** Context key for tracking executed finally blocks. */
data object TryFinallyKey : CooperationContext.MappedKey<TryFinallyElement>()

/**
 * Context element that tracks which finally blocks have been executed.
 *
 * This element maintains a list of step names for which the finally block has already been
 * executed. The [plus] method ensures that when contexts are combined, the lists of executed
 * finally blocks are merged.
 *
 * @param finallyRunForSteps List of step names whose finally blocks have been executed
 */
data class TryFinallyElement(val finallyRunForSteps: List<String>) :
    CooperationContext.MappedElement(TryFinallyKey) {
    override fun plus(context: CooperationContext): CooperationContext =
        when (context) {
            is TryFinallyElement ->
                TryFinallyElement(finallyRunForSteps + context.finallyRunForSteps)
            else -> super.plus(context)
        }
}

/**
 * Checks if the finally block for the given step has already been executed.
 *
 * @param name The step name to check
 * @return true if the finally block has been executed, false otherwise
 */
fun CooperationScope.finallyRun(name: String) =
    context[TryFinallyKey]?.finallyRunForSteps?.contains(name) ?: false

/**
 * Marks the finally block for the given step as executed.
 *
 * This updates the [CooperationContext] to track that the finally block has run, preventing it from
 * running again during rollback scenarios.
 *
 * @param name The step name to mark as executed
 */
fun CooperationScope.markFinallyRun(name: String) {
    context += TryFinallyElement(listOf(name))
}

/**
 * Creates a try-finally step pattern in a saga.
 *
 * This function creates two actual saga steps that implement try-finally semantics:
 * 1. **Try step**: Executes the [invoke] block with exception handling
 * 2. **Finally step**: Executes the [finally] block if not already executed
 *
 * ## Execution Flow
 *
 * **Normal execution**:
 * 1. Try step runs [invoke] block
 * 2. If [invoke] succeeds, saga suspends until child handlers complete
 * 3. Finally step runs [finally] block
 *
 * **Exception in invoke**:
 * 1. Try step runs [invoke] block
 * 2. If [invoke] throws, [finally] block runs immediately
 * 3. Exception is re-thrown (triggering rollback)
 *
 * **Rollback scenario**:
 * 1. Try step's rollback checks if [finally] has run
 * 2. If not, [finally] block runs during rollback
 * 3. Step is marked as having run its finally block
 *
 * @param invoke The main logic to execute (equivalent to try block)
 * @param finally The cleanup logic to execute (equivalent to finally block)
 */
fun SagaBuilder.tryFinallyStep(
    invoke: (CooperationScope, Message) -> Unit,
    finally: (CooperationScope, Message) -> Unit,
) {
    val name = steps.size.toString()
    step(
        invoke = { scope, message ->
            try {
                invoke(scope, message)
            } catch (e: Exception) {
                // In case the handler itself throws, and not a child scope,
                // we still want to run the `finally` block, but there's no point
                // in doing any checks or markings, since we never leave the step
                finally(scope, message)
                throw e
            }
        },
        rollback = { scope, message, throwable ->
            if (!scope.finallyRun(name)) {
                scope.markFinallyRun(name)
                finally(scope, message)
            }
        },
    )

    step { scope, message ->
        if (!scope.finallyRun(name)) {
            scope.markFinallyRun(name)
            finally(scope, message)
        }
    }
}

package io.github.gabrielshanahan.scoop.reactive.coroutine.continuation

import io.github.gabrielshanahan.scoop.reactive.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import io.vertx.mutiny.sqlclient.SqlConnection

/**
 * A continuation for normal (forward) execution of saga steps.
 *
 * This continuation handles the "happy path" - when sagas are executing their steps in the normal
 * forward direction (as opposed to rolling back). It executes the
 * [TransactionalStep.invoke][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.invoke]
 * methods and handles child failures via
 * [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep.handleChildFailures].
 *
 * However, all this is done in the parent [BaseCooperationContinuation]. Here, we only specialize
 * the [giveUpStrategy], as that's all that's needed.
 */
internal class HappyPathContinuation(
    connection: SqlConnection,
    context: CooperationContext,
    cooperationScopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    scopeCapabilities: ScopeCapabilities,
) :
    BaseCooperationContinuation(
        connection,
        context,
        cooperationScopeIdentifier,
        suspensionPoint,
        distributedCoroutine,
        scopeCapabilities,
    ) {
    override fun giveUpStrategy(seen: String): String =
        distributedCoroutine.eventLoopStrategy.giveUpOnHappyPath(seen)
}

/**
 * Builds a [HappyPathContinuation] from the current saga state.
 *
 * This function analyzes the saga's current state and creates the appropriate continuation to
 * resume execution from where it left off. It determines the correct [SuspensionPoint] based on
 * what step was last executed.
 *
 * The logic handles three cases:
 * - **Not suspended yet**: Create a continuation to execute the first step
 * - **Suspended after a step**: Create a continuation to execute the next step
 * - **Suspended after last step**: Create a continuation that will complete the saga
 *
 * @param connection Reactive database connection for the continuation's transaction
 * @param coroutineState Current state of the saga run (which step, context, etc.)
 * @param scopeCapabilities Provides capabilities exposed by
 *   [CooperationScope][io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope]
 * @return A happy path continuation ready to resume execution
 */
internal fun DistributedCoroutine.buildHappyPathContinuation(
    connection: SqlConnection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
) =
    when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // No SUSPEND record, so we've just started processing this message
            HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                this,
                scopeCapabilities,
            )
        }

        is LastSuspendedStep.SuspendedAfter -> {
            val suspendedStepIdx =
                steps.indexOfFirst { it.name == coroutineState.lastSuspendedStep.stepName }

            check(suspendedStepIdx > -1) {
                "Step ${coroutineState.lastSuspendedStep} was not found"
            }

            if (steps[suspendedStepIdx] == steps.last()) {
                HappyPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    this,
                    scopeCapabilities,
                )
            } else {
                HappyPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.BetweenSteps(
                        steps[suspendedStepIdx],
                        steps[suspendedStepIdx + 1],
                    ),
                    this,
                    scopeCapabilities,
                )
            }
        }
    }

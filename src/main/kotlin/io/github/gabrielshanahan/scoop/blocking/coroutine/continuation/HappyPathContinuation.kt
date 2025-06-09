package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import java.sql.Connection

/**
 * A continuation for normal (forward) execution of saga steps.
 *
 * This continuation handles the "happy path" - when sagas are executing their steps in the normal
 * forward direction (as opposed to rolling back). It executes the
 * [TransactionalStep.invoke][io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep.invoke]
 * methods and handles child failures via
 * [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep.handleChildFailures].
 *
 * However, all this is done in the parent [BaseCooperationContinuation]. Here, we only specialize
 * the [giveUpStrategy], as that's all that's needed.
 */
internal class HappyPathContinuation(
    connection: Connection,
    context: CooperationContext,
    scopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    scopeCapabilities: ScopeCapabilities,
) :
    BaseCooperationContinuation(
        connection,
        context,
        scopeIdentifier,
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
 * @param connection Database connection for the continuation's transaction
 * @param coroutineState Current state of the saga run (which step, context, etc.)
 * @param scopeCapabilities Provides capabilities exposed by
 *   [CooperationScope][io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope]
 * @return A happy path continuation ready to resume execution
 */
internal fun DistributedCoroutine.buildHappyPathContinuation(
    connection: Connection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
) =
    when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // Case 1: Not suspended yet - Create a continuation to execute the first step
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
                // Case 2: Suspended after last step - Create a continuation that will complete the
                // saga
                HappyPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.AfterLastStep(steps.last()),
                    this,
                    scopeCapabilities,
                )
            } else {
                // Case 3: Suspended after a step - Create a continuation to execute the next step
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

package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.coroutine.eventloop.ChildFailureHandlerIteration
import io.github.gabrielshanahan.scoop.coroutine.eventloop.SuspensionState
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ScopeCapabilities
import java.sql.Connection

/**
 * A continuation for normal (forward) execution of saga steps.
 *
 * This continuation handles the "happy path" - when sagas are executing their steps in the normal
 * forward direction (as opposed to rolling back). It executes the
 * [TransactionalStep.invoke][io.github.gabrielshanahan.scoop.coroutine.TransactionalStep.invoke]
 * methods and handles child failures via
 * [TransactionalStep.handleChildFailures][io.github.gabrielshanahan.scoop.coroutine.TransactionalStep.handleChildFailures].
 *
 * However, all this is done in the parent [BaseCooperationContinuation]. Here, we only specialize
 * the [giveUpStrategy], as that's all that's needed.
 */
@Suppress("LongParameterList")
internal class HappyPathContinuation(
    connection: Connection,
    context: CooperationContext,
    scopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    scopeCapabilities: ScopeCapabilities,
    stepIteration: Int,
    childFailureHandlerIteration: ChildFailureHandlerIteration,
) :
    BaseCooperationContinuation(
        connection,
        context,
        scopeIdentifier,
        suspensionPoint,
        distributedCoroutine,
        scopeCapabilities,
        stepIteration = stepIteration,
        childFailureHandlerIteration = childFailureHandlerIteration,
    ) {
    override fun giveUpStrategy(seen: String): String =
        distributedCoroutine.eventLoopStrategy.giveUpOnHappyPath(seen)
}

/**
 * Builds a [HappyPathContinuation] from the current saga state.
 *
 * This function analyzes the saga's current state and creates the appropriate continuation to
 * resume execution from where it left off. It determines the correct [SuspensionPoint] based on
 * what step was last executed and what step is supposed to execute next.
 *
 * The logic handles three cases:
 * - **Not suspended yet**: Create a continuation to execute the first step
 * - **Last step finished**: Create a continuation that will complete the saga
 * - **Suspended between steps**: Create a continuation to execute the next step (which may be the
 *   same step if [NextStep.Repeat][io.github.gabrielshanahan.scoop.coroutine.NextStep.Repeat] or
 *   [NextStep.GoTo][io.github.gabrielshanahan.scoop.coroutine.NextStep.GoTo] was returned)
 *
 * @param connection Database connection for the continuation's transaction
 * @param coroutineState Current state of the saga run (which step, context, etc.)
 * @param scopeCapabilities Provides capabilities exposed by
 *   [CooperationScope][io.github.gabrielshanahan.scoop.coroutine.CooperationScope]
 * @return A happy path continuation ready to resume execution
 */
internal fun DistributedCoroutine.buildHappyPathContinuation(
    connection: Connection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
) =
    when (coroutineState.suspensionState) {
        is SuspensionState.NotSuspendedYet -> {
            // Case 1: Not suspended yet - Create a continuation to execute the first step
            HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.BeforeFirstStep(steps.first()),
                this,
                scopeCapabilities,
                stepIteration = 0,
                childFailureHandlerIteration = ChildFailureHandlerIteration.NoChildFailure,
            )
        }

        is SuspensionState.LastStepFinished -> {
            // Case 2: Last step finished - Create completion continuation
            val lastSuspended = coroutineState.suspensionState
            HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(steps.last()),
                this,
                scopeCapabilities,
                stepIteration = coroutineState.stepIteration,
                childFailureHandlerIteration = lastSuspended.childFailureHandlerIteration,
            )
        }

        is SuspensionState.SuspendedBetweenSteps -> {
            val lastSuspended = coroutineState.suspensionState
            val completedStepIdx = steps.indexOfFirst { it.name == lastSuspended.completedStep }
            val nextStepIdx = steps.indexOfFirst { it.name == lastSuspended.nextStep }
            check(completedStepIdx in steps.indices) {
                "Step ${lastSuspended.completedStep} was not found"
            }
            check(nextStepIdx in steps.indices) {
                "Next step ${lastSuspended.nextStep} was not found"
            }

            // Case 3: Normal forward execution - Create continuation for next step
            HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.BetweenSteps(steps[completedStepIdx], steps[nextStepIdx]),
                this,
                scopeCapabilities,
                stepIteration = coroutineState.stepIteration,
                childFailureHandlerIteration = lastSuspended.childFailureHandlerIteration,
            )
        }

        is SuspensionState.SuspendedAfterStepRollback ->
            error("buildHappyPathContinuation called with SuspendedAfterStepRollback state")
    }

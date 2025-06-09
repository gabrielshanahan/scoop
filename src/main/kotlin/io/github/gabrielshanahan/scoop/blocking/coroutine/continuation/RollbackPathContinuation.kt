package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import java.sql.Connection

/**
 * A continuation for rollback (compensating) execution of saga steps.
 * 
 * This continuation handles rollback scenarios - when sagas need to "undo" their actions
 * due to failures in later steps or child handlers. It executes the [TransactionalStep.rollback]
 * methods in reverse order from the failed step back to the first step.
 * 
 * The rollback continuation moves through the saga's steps in reverse order:
 * 1. Execute step N's [TransactionalStep.rollback] method  
 * 2. Suspend and wait for child rollbacks to complete
 * 3. Move to step N-1 and repeat
 * 4. When all steps are rolled back, mark the saga as rolled back
 * 
 * @see HappyPathContinuation for normal forward execution
 * @see BaseCooperationContinuation for shared continuation logic
 */

// TODO: Config
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

internal class RollbackPathContinuation(
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
        distributedCoroutine.eventLoopStrategy.giveUpOnRollbackPath(seen)
}

internal fun DistributedCoroutine.buildRollbackPathContinuation(
    connection: Connection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
): RollbackPathContinuation {
    val rollingBackPrefix = run {
        var prefix = ROLLING_BACK_PREFIX
        while (steps.any { it.name.startsWith(prefix) }) {
            prefix += "$"
        }
        prefix
    }
    val rollingBackSuffix = run {
        var suffix = ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
        while (steps.any { it.name.endsWith(suffix) }) {
            suffix += "$"
        }
        suffix
    }

    val rollbackSteps =
        buildRollbackSteps(rollingBackPrefix, rollingBackSuffix, scopeCapabilities)

    return when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // We're rolling back before the first step committed. Since the
            // transaction itself was never committed, nothing was emitted, and we're done.
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                this,
                scopeCapabilities,
            )
        }

        is LastSuspendedStep.SuspendedAfter -> {

            val cleanLastSuspendedStepName =
                coroutineState.lastSuspendedStep.stepName
                    .removePrefix(rollingBackPrefix)
                    .removeSuffix(rollingBackSuffix)

            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            check(stepToRevert > -1) { "Step $cleanLastSuspendedStepName was not found" }

            // This means "rollback just started" and it allows us to have this logic here, instead
            // of in SQL. Although adding a "rollback is the last event" check could be made part
            // of the SQL, since it's similar to what we do with contexts anyway
            if (cleanLastSuspendedStepName == coroutineState.lastSuspendedStep.stepName) {
                // We've doubled the number of steps by transforming each step into two substeps, so
                // we multiply by two, and we want to start with the last one (reverting child
                // scopes), so we add one
                val nextStepIdx = 2 * stepToRevert + 1
                RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps[nextStepIdx]),
                    this,
                    scopeCapabilities,
                )
            } else {
                val nextStepIdx =
                    rollbackSteps.indexOfFirst {
                        it.name == coroutineState.lastSuspendedStep.stepName
                    }
                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        this,
                        scopeCapabilities,
                    )
                } else {
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.BetweenSteps(
                            rollbackSteps[nextStepIdx],
                            rollbackSteps[nextStepIdx - 1],
                        ),
                        this,
                        scopeCapabilities,
                    )
                }
            }
        }
    }
}

private fun DistributedCoroutine.buildRollbackSteps(
    rollingBackPrefix: String,
    rollingBackSuffix: String,
    scopeCapabilities: ScopeCapabilities
): List<TransactionalStep> =
    steps.flatMap { step ->
        listOf(
            object : TransactionalStep {
                override val name: String
                    get() = rollingBackPrefix + step.name

                override fun invoke(scope: CooperationScope, message: Message) =
                    step.invoke(scope, message)

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) = step.rollback(scope, message, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) = step.handleChildFailures(scope, message, throwable)
            },
            object : TransactionalStep {
                override val name: String
                    get() = rollingBackPrefix + step.name + rollingBackSuffix

                override fun invoke(scope: CooperationScope, message: Message) =
                    error("Should never be invoked")

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) =
                    scopeCapabilities.emitRollbacksForEmissions(
                        scope,
                        step.name,
                        throwable,
                    )

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ) = step.handleChildFailures(scope, message, throwable)
            },
        )
    }

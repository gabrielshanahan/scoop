package io.github.gabrielshanahan.scoop.reactive.coroutine.continuation

import io.github.gabrielshanahan.scoop.reactive.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.reactive.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.reactive.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.reactive.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.reactive.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.LastSuspendedStep
import io.smallrye.mutiny.Uni
import io.vertx.mutiny.sqlclient.SqlConnection

/**
 * A continuation for rollback (compensating) execution of saga steps.
 *
 * This continuation handles rollback scenarios - when sagas need to "undo" their actions due to
 * failures in later steps or child handlers.
 *
 * ## Two-Phase Rollback Process
 *
 * **Important insight**: Each original [TransactionalStep] is split into **two steps** during
 * rollback:
 * 1. **Child Rollback Step**:
 *     - step name = `"Rollback of <step name> (rolling back child scopes)"`
 *     - Emits rollback messages to all child handlers that were spawned by this step
 *     - Does NOT execute the step's [TransactionalStep.rollback] method yet
 *     - Waits for all child handlers to complete their own rollbacks
 * 2. **Self Rollback Step**:
 *     - step name = `"Rollback of <step name>"`
 *     - Only executes after ALL children have finished rolling back
 *     - Executes the step's [TransactionalStep.rollback] method
 *     - Performs the actual compensating action for this step
 *
 * ## Why Two Phases?
 *
 * This two-phase approach ensures proper rollback ordering in distributed systems:
 * - **Child-first ordering**: Children must roll back before their parents
 * - **Structured unwinding**: Mirrors exception stack unwinding but across service boundaries
 * - **Consistency guarantees**: No step rolls back until all its dependent work is undone
 *
 * ## Rollback Execution Flow
 *
 * For a 3-step saga `[Step1, Step2, Step3]` that fails during Step3:
 * ```
 * Original Steps:     Step1 → Step2 → Step3 (fails)
 * Rollback Sub-steps: Step3₁ → Step3₂ → Step2₁ → Step2₂ → Step1₁ → Step1₂
 * ```
 *
 * Where:
 * - `Step3₁` = "Rollback of Step3 (rolling back child scopes)" - emit child rollbacks
 * - `Step3₂` = "Rollback of Step3" - execute Step3.rollback()
 * - `Step2₁` = "Rollback of Step2 (rolling back child scopes)" - emit child rollbacks
 * - `Step2₂` = "Rollback of Step2" - execute Step2.rollback()
 * - etc.
 *
 * ## Implementation Details
 *
 * The rollback continuation processes these sub-steps in sequence, suspending between each to wait
 * for the
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
 * to signal readiness (typically when all child handlers have completed).
 *
 * Step name generation uses unique prefixes/suffixes to avoid conflicts with user-defined step
 * names, automatically adding '$' characters if collisions are detected.
 *
 * @see HappyPathContinuation for normal forward execution
 * @see BaseCooperationContinuation for shared continuation logic
 * @see buildRollbackSteps for the sub-step generation logic
 */

// TODO: Config
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

internal class RollbackPathContinuation(
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
        distributedCoroutine.eventLoopStrategy.giveUpOnRollbackPath(seen)
}

/**
 * Builds a [RollbackPathContinuation] from the current saga state for rollback execution.
 *
 * This transforms the saga's linear step sequence into a two-phase rollback execution plan and
 * determines exactly where to resume rollback based on the current state.
 *
 * ## High-Level Process
 * 1. **Generate unique prefixes/suffixes** to avoid naming conflicts with user steps
 * 2. **Transform steps into rollback sub-steps** (2 sub-steps per original step, see above)
 * 3. **Determine rollback entry point** based on saga's current execution state
 * 4. **Create continuation** with the appropriate suspension point
 *
 * ## Step Transformation
 *
 * Each original step becomes two rollback sub-steps:
 * ```
 * Original: "PaymentStep"
 * Becomes:  ["Rollback of PaymentStep (rolling back child scopes)", "Rollback of PaymentStep"]
 * ```
 *
 * ## Entry Point Logic
 *
 * **Case 1: NotSuspendedYet** - Rollback during the first step, before anything was actually
 * committed
 * - No actual work was persisted, so we're immediately done
 * - Creates continuation positioned after the last rollback step (completion)
 *
 * **Case 2: SuspendedAfter** - Rollback after some step(s) executed
 * - **Sub-case 2a**: Rollback just initiated (last step name has no rollback prefix/suffix)
 *     - Start with the "child rollback" sub-step for the failed step
 *     - Uses formula: `nextStepIdx = 2 * stepToRevertIdx + 1` (explained bellow)
 * - **Sub-case 2b**: Rollback already in progress (step name has rollback prefix/suffix)
 *     - Resume from the next rollback sub-step in sequence
 *     - May be completing rollback if we're on the first rollback step
 *
 * ## Collision Avoidance
 *
 * The method automatically detects if user-defined steps conflict with generated rollback step
 * names and appends '$' characters until uniqueness is achieved.
 *
 * @param connection Database connection for the continuation's transaction
 * @param coroutineState Current saga state (which step, context, etc.)
 * @param scopeCapabilities Provides capabilities for scope operations
 * @return A rollback continuation positioned to resume at the correct rollback sub-step
 * @see buildRollbackSteps for the step transformation logic
 */
internal fun DistributedCoroutine.buildRollbackPathContinuation(
    connection: SqlConnection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
): RollbackPathContinuation {
    // Phase 1: Generate unique prefixes/suffixes to avoid conflicts with user-defined step names
    // If a user step is named "Rollback of SomeStep", we append '$' until it's unique
    val rollingBackPrefix = run {
        var prefix = ROLLING_BACK_PREFIX // "Rollback of "
        while (steps.any { it.name.startsWith(prefix) }) {
            prefix += "$" // Becomes "Rollback of $", "Rollback of $$", etc.
        }
        prefix
    }
    val rollingBackSuffix = run {
        var suffix = ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX // " (rolling back child scopes)"
        while (steps.any { it.name.endsWith(suffix) }) {
            suffix += "$" // Becomes " (rolling back child scopes)$", etc.
        }
        suffix
    }

    // Phase 2: Transform original steps into rollback sub-steps
    // Each original step becomes 2 rollback sub-steps (child rollback + self rollback)
    val rollbackSteps = buildRollbackSteps(rollingBackPrefix, rollingBackSuffix, scopeCapabilities)

    // Phase 3: Determine where to start continuation based on current saga state
    return when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // Case 1: Rollback before any step committed to database
            // Since no transaction was committed, no messages were emitted, no child handlers
            // were spawned, and there's nothing to roll back. Position after the last rollback
            // step so the continuation immediately completes the saga.
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
            // Case 2: Rollback after one or more steps executed and committed

            // Extract the original step name by removing any rollback prefixes/suffixes
            // E.g., "Rollback of PaymentStep (rolling back child scopes)" → "PaymentStep"
            val cleanLastSuspendedStepName =
                coroutineState.lastSuspendedStep.stepName
                    .removePrefix(rollingBackPrefix)
                    .removeSuffix(rollingBackSuffix)

            // Find the index of the original step in the saga definition
            val stepToRevert = steps.indexOfFirst { it.name == cleanLastSuspendedStepName }

            check(stepToRevert > -1) { "Step $cleanLastSuspendedStepName was not found" }

            // Determine if rollback just started vs. rollback already in progress
            // Key insight: If the clean name equals the original name, then the suspended step
            // has no rollback prefix/suffix, meaning we're transitioning from normal execution
            // to rollback execution for the first time.
            // This could be determined directly in the "fetchCoroutineState" SQL by evaluating
            // "is ROLLING_BACK the last event that was written", but this works as well.
            if (cleanLastSuspendedStepName == coroutineState.lastSuspendedStep.stepName) {
                // Sub-case 2a: Rollback just started (normal step name, no rollback prefix/suffix)
                // Need to start rollback for the failed step. Each original step becomes 2 rollback
                // sub-steps, so we use the following formula to find the "child rollback" sub-step.
                // Formula: For original step at index N, the "child rollback" sub-step is at index
                // 2*N+1
                // Example:
                // Original steps: ["step A", "step B", "step C"]
                // Original indices: [0, 1, 2]
                // Rollback steps: ["Rollback of step A (rolling back child scopes)", "Rollback of
                // step A",
                //                  "Rollback of step B (rolling back child scopes)", "Rollback of
                // step B",
                //                  "Rollback of step C (rolling back child scopes)", "Rollback of
                // step C"]
                // Rollback indices: [0, 1, 2, 3, 4, 5]
                //
                // If step C (index 2) failed, we want to start with "Rollback of step C (rolling
                // back child scopes)"
                // Formula: 2 * 2 + 1 = 5, which gives us rollbackSteps[5] = "Rollback of step C
                // (rolling back child scopes)"
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
                // Sub-case 2b: Rollback already in progress (step name has rollback prefix/suffix)
                // Find the current rollback sub-step in the rollback sequence
                val nextStepIdx =
                    rollbackSteps.indexOfFirst {
                        it.name == coroutineState.lastSuspendedStep.stepName
                    }

                if (rollbackSteps[nextStepIdx] == rollbackSteps.first()) {
                    // We're on the first (i.e., last in rollback execution order) step - rollback
                    // is completing
                    // Position after the last rollback step so the continuation completes the saga
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.AfterLastStep(rollbackSteps.first()),
                        this,
                        scopeCapabilities,
                    )
                } else {
                    // Rollback continues - move to the next rollback sub-step in the sequence
                    // Note: rollbackSteps are in reverse execution order, so we subtract 1 to get
                    // "next"
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

/**
 * Transforms the original saga steps into rollback sub-steps for two-phase rollback execution. Each
 * original step becomes two rollback sub-steps that execute in reverse order to ensure child-first
 * rollback semantics.
 *
 * ## Transformation Logic
 *
 * For each original step, creates two rollback sub-steps:
 * 1. **Self Rollback Step**: `"<prefix>StepName"`
 *     - Executes the original step's [TransactionalStep.rollback] method
 * 2. **Child Rollback Step**: `"<prefix>StepName<suffix>"`
 *     - Emits rollback messages to all child handlers via
 *       [ScopeCapabilities.emitRollbacksForEmissions]
 *
 * The caller ensures that these are called in reverse order, so that each step's children roll back
 * before the step itself rolls back.
 *
 * @param rollingBackPrefix Unique prefix for rollback step names (e.g., "Rollback of ")
 * @param rollingBackSuffix Unique suffix for child rollback steps (e.g., " (rolling back child
 *   scopes)")
 * @param scopeCapabilities Provides the [ScopeCapabilities.emitRollbacksForEmissions] functionality
 * @return List of rollback sub-steps in reverse execution order (last step's sub-steps first)
 */
private fun DistributedCoroutine.buildRollbackSteps(
    rollingBackPrefix: String,
    rollingBackSuffix: String,
    scopeCapabilities: ScopeCapabilities,
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
                ): Uni<Unit> = step.rollback(scope, message, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ): Uni<Unit> = step.handleChildFailures(scope, message, throwable)
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
                ): Uni<Unit> =
                    scopeCapabilities.emitRollbacksForEmissions(scope, step.name, throwable)

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                ): Uni<Unit> = step.handleChildFailures(scope, message, throwable)
            },
        )
    }

package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.ChildFailureContext
import io.github.gabrielshanahan.scoop.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.StepInstance
import io.github.gabrielshanahan.scoop.coroutine.StepResult
import io.github.gabrielshanahan.scoop.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.coroutine.eventloop.LastSuspendedStep
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.messaging.Message
import java.sql.Connection

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
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.coroutine.eventloop.strategy.EventLoopStrategy]
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

@Suppress("LongParameterList")
internal class RollbackPathContinuation(
    connection: Connection,
    context: CooperationContext,
    scopeIdentifier: CooperationScopeIdentifier.Child,
    suspensionPoint: SuspensionPoint,
    distributedCoroutine: DistributedCoroutine,
    scopeCapabilities: ScopeCapabilities,
    initialIteration: Int = 0,
    initialCfhi: Int? = null,
) :
    BaseCooperationContinuation(
        connection,
        context,
        scopeIdentifier,
        suspensionPoint,
        distributedCoroutine,
        scopeCapabilities,
        currentIteration = initialIteration,
        currentCfhi = initialCfhi,
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
    connection: Connection,
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

    // Phase 2: Filter out rollback step names from executed instances (only keep original steps)
    val originalInstances =
        coroutineState.executedStepInstances.filter { instance ->
            !instance.step.startsWith(rollingBackPrefix)
        }

    // Phase 3: Transform executed step instances into rollback sub-steps
    // Each executed instance becomes 2 rollback sub-steps (child rollback + self rollback)
    val rollbackSteps =
        buildRollbackSteps(
            rollingBackPrefix,
            rollingBackSuffix,
            scopeCapabilities,
            originalInstances,
        )

    // Helper: create a sentinel rollback step with the proper rollback name for immediate
    // completion. Used when no executed instances exist (step threw before committing or
    // handler never started its first step).
    fun createSentinelRollbackStep(
        stepName: String,
        iteration: Int,
        cfhi: Int?,
    ): TransactionalStep {
        val syntheticRollbackSteps =
            buildRollbackSteps(
                rollingBackPrefix,
                rollingBackSuffix,
                scopeCapabilities,
                listOf(StepInstance(stepName, iteration, cfhi)),
            )
        return if (syntheticRollbackSteps.isNotEmpty()) {
            syntheticRollbackSteps.last()
        } else {
            steps.first()
        }
    }

    // Phase 4: Determine where to start continuation based on current saga state
    return when (coroutineState.lastSuspendedStep) {
        is LastSuspendedStep.NotSuspendedYet -> {
            // Case 1: Rollback before any step committed to database
            // Nothing to roll back - position after the last step so continuation completes.
            // Use a synthetic rollback step so ROLLED_BACK event records the proper name.
            val sentinel = createSentinelRollbackStep(steps.first().name, 0, null)
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(sentinel),
                this,
                scopeCapabilities,
            )
        }

        is LastSuspendedStep.SuspendedAfter -> {
            // Case 2: Rollback after one or more steps executed and committed
            val lastStepName = coroutineState.lastSuspendedStep.stepName
            val hasRollbackPrefix = lastStepName.startsWith(rollingBackPrefix)

            if (rollbackSteps.isEmpty()) {
                // No executed instances to roll back (step threw before committing).
                // Use a synthetic rollback step so ROLLED_BACK event records the proper name.
                val lastSuspended = coroutineState.lastSuspendedStep
                val sentinel =
                    createSentinelRollbackStep(
                        lastSuspended.stepName,
                        lastSuspended.iteration,
                        lastSuspended.childFailureHandlerIteration,
                    )
                RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.AfterLastStep(sentinel),
                    this,
                    scopeCapabilities,
                )
            } else if (!hasRollbackPrefix) {
                // Sub-case 2a: Rollback just started (normal step name)
                // Start with the first rollback sub-step (child rollback of last executed instance)
                RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.BeforeFirstStep(rollbackSteps.first()),
                    this,
                    scopeCapabilities,
                )
            } else {
                // Sub-case 2b: Rollback already in progress
                val nextStepIdx = rollbackSteps.indexOfFirst { it.name == lastStepName }

                check(nextStepIdx > -1) { "Rollback step $lastStepName was not found" }

                if (nextStepIdx == rollbackSteps.lastIndex) {
                    // We're on the last rollback sub-step - rollback is completing
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.AfterLastStep(rollbackSteps.last()),
                        this,
                        scopeCapabilities,
                    )
                } else {
                    // Rollback continues - move to the next rollback sub-step
                    RollbackPathContinuation(
                        connection,
                        coroutineState.cooperationContext,
                        coroutineState.scopeIdentifier,
                        SuspensionPoint.BetweenSteps(
                            rollbackSteps[nextStepIdx],
                            rollbackSteps[nextStepIdx + 1],
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
 * Transforms executed step instances into rollback sub-steps for two-phase rollback execution. Each
 * executed instance becomes two rollback sub-steps that execute in reverse order to ensure
 * child-first rollback semantics.
 *
 * ## Transformation Logic
 *
 * For each executed step instance `(stepName, iteration, cfhi)`, creates two rollback sub-steps:
 * 1. **Self Rollback Step**: `"<prefix>StepName[iteration,cfhi]"`
 *     - Executes the original step's [TransactionalStep.rollback] method with the correct iteration
 *       and [ChildFailureContext]
 * 2. **Child Rollback Step**: `"<prefix>StepName[iteration,cfhi]<suffix>"`
 *     - Emits rollback messages to all child handlers spawned by this specific instance via
 *       [ScopeCapabilities.emitRollbacksForEmissions]
 *
 * The instances are already in reverse execution order, so children roll back before parents.
 *
 * @param rollingBackPrefix Unique prefix for rollback step names (e.g., "Rollback of ")
 * @param rollingBackSuffix Unique suffix for child rollback steps (e.g., " (rolling back child
 *   scopes)")
 * @param scopeCapabilities Provides the [ScopeCapabilities.emitRollbacksForEmissions] functionality
 * @param executedInstances List of step instances in reverse execution order
 * @return List of rollback sub-steps in reverse execution order
 */
private fun DistributedCoroutine.buildRollbackSteps(
    rollingBackPrefix: String,
    rollingBackSuffix: String,
    scopeCapabilities: ScopeCapabilities,
    executedInstances: List<StepInstance>,
): List<TransactionalStep> =
    executedInstances.flatMap { instance ->
        val step = steps.first { it.name == instance.step }
        val instanceSuffix =
            "[${instance.iteration},${instance.childFailureHandlerIteration ?: ""}]"
        val childFailureContext =
            if (instance.childFailureHandlerIteration != null) {
                ChildFailureContext.ChildFailureHandlerRun(instance.childFailureHandlerIteration)
            } else {
                ChildFailureContext.NotChildFailureHandler
            }

        val childRollbackStep =
            // Child rollback step: emits rollback messages to children of this instance
            object : TransactionalStep {
                override val name: String
                    get() = rollingBackPrefix + step.name + instanceSuffix + rollingBackSuffix

                override fun invoke(
                    scope: CooperationScope,
                    message: Message,
                    iteration: Int,
                ): StepResult = error("Should never be invoked")

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    iteration: Int,
                    childFailureCtx: ChildFailureContext,
                ) =
                    scopeCapabilities.emitRollbacksForEmissions(
                        scope,
                        step.name,
                        instance.iteration,
                        instance.childFailureHandlerIteration,
                        throwable,
                    )

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    iteration: Int,
                    childFailureHandlerIteration: Int,
                ) =
                    step.handleChildFailures(
                        scope,
                        message,
                        throwable,
                        iteration,
                        childFailureHandlerIteration,
                    )
            }

        // Only create a self-rollback step for the original invocation (cfhi == null).
        // handleChildFailures runs (cfhi != null) only need their children rolled back,
        // not the step itself rolled back again.
        if (instance.childFailureHandlerIteration != null) {
            listOf(childRollbackStep)
        } else {
            val selfRollbackStep =
                // Self rollback step: executes the original step's rollback method
                object : TransactionalStep {
                    override val name: String
                        get() = rollingBackPrefix + step.name + instanceSuffix

                    override fun invoke(
                        scope: CooperationScope,
                        message: Message,
                        iteration: Int,
                    ): StepResult = step.invoke(scope, message, iteration)

                    override fun rollback(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                        iteration: Int,
                        childFailureCtx: ChildFailureContext,
                    ) =
                        step.rollback(
                            scope,
                            message,
                            throwable,
                            instance.iteration,
                            childFailureContext,
                        )

                    override fun handleChildFailures(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                        iteration: Int,
                        childFailureHandlerIteration: Int,
                    ) =
                        step.handleChildFailures(
                            scope,
                            message,
                            throwable,
                            iteration,
                            childFailureHandlerIteration,
                        )
                }
            listOf(childRollbackStep, selfRollbackStep)
        }
    }

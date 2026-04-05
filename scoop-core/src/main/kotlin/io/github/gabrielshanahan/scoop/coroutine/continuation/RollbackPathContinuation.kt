package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.coroutine.CoroutineState
import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.coroutine.NextStep
import io.github.gabrielshanahan.scoop.coroutine.StepInstance
import io.github.gabrielshanahan.scoop.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.coroutine.eventloop.ChildFailureHandlerIteration
import io.github.gabrielshanahan.scoop.coroutine.eventloop.SuspensionState
import io.github.gabrielshanahan.scoop.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.messaging.Message
import java.sql.Connection
import java.time.Instant
import org.slf4j.LoggerFactory

/**
 * A continuation for rollback (compensating) execution of saga steps.
 *
 * This continuation handles rollback scenarios - when sagas need to "undo" their actions due to
 * failures in later steps or child handlers.
 *
 * ## Two-Phase Rollback Process
 *
 * **Important insight**: Each executed step instance is split into **two synthetic steps** during
 * rollback:
 * 1. **Child Rollback Step**:
 *     - step name = `"Rollback of <step name>[<iterationIndex>,<childFailureHandlerIteration>]
 *       (rolling back child scopes)"`
 *     - Emits rollback messages to all child handlers that were spawned by this step instance
 *     - Does NOT execute the step's [TransactionalStep.rollback] method yet
 *     - Waits for all child handlers to complete their own rollbacks
 * 2. **Self Rollback Step**:
 *     - step name = `"Rollback of <step name>[<iterationIndex>,<childFailureHandlerIteration>]"`
 *     - Only executes after ALL children have finished rolling back
 *     - Executes the step's [TransactionalStep.rollback] method
 *     - Performs the actual compensating action for this step instance
 *
 * For step instances that were `handleChildFailures` runs (i.e., have a non-null
 * [childFailureHandlerIteration][StepInstance.childFailureHandlerIteration] is a
 * [HandlerIteration][ChildFailureHandlerIteration.HandlerIteration]), only the child rollback step
 * is generated (since there is no self-rollback to perform - the step's own invoke was not called
 * during a child failure handler run).
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
 * ## Instance-Based Rollback
 *
 * Unlike the happy path which operates on step definitions, the rollback path operates on
 * **executed step instances**. This is critical when steps execute multiple times (via
 * [NextStep.Repeat] or [NextStep.GoTo]), because each execution may have spawned different child
 * scopes that all need to be rolled back individually.
 *
 * The instance suffix `[iterationIndex,childFailureHandlerIteration]` ensures unique naming. For
 * example, if "PaymentStep" executed 3 times, the rollback steps would be:
 * ```
 * "Rollback of PaymentStep[2,] (rolling back child scopes)"
 * "Rollback of PaymentStep[2,]"
 * "Rollback of PaymentStep[1,] (rolling back child scopes)"
 * "Rollback of PaymentStep[1,]"
 * "Rollback of PaymentStep[0,] (rolling back child scopes)"
 * "Rollback of PaymentStep[0,]"
 * ```
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
 * However, all the continuation logic is inherited from [BaseCooperationContinuation]. Here, we
 * only specialize the [giveUpStrategy], as that's all that's needed.
 *
 * @see HappyPathContinuation for normal forward execution
 * @see BaseCooperationContinuation for shared continuation logic
 * @see buildRollbackSteps for the sub-step generation logic
 */
const val ROLLING_BACK_PREFIX = "Rollback of "
const val ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

private val rollbackLogger =
    LoggerFactory.getLogger(
        "io.github.gabrielshanahan.scoop.coroutine.continuation.RollbackPathContinuation"
    )

@Suppress("LongParameterList")
internal class RollbackPathContinuation(
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
        distributedCoroutine.eventLoopStrategy.giveUpOnRollbackPath(seen)
}

/**
 * Builds a [RollbackPathContinuation] from the current saga state for rollback execution.
 *
 * This transforms the saga's step execution history into a two-phase rollback execution plan and
 * determines exactly where to resume rollback based on the current state. It examines the history
 * of executed step instances to construct an instance-aware rollback plan — critical for correctly
 * rolling back steps that executed multiple times (via [NextStep.Repeat] or [NextStep.GoTo]), and
 * also any child failure handler executions.
 *
 * ## High-Level Process
 * 1. **Generate unique prefixes/suffixes** to avoid naming conflicts with user steps
 * 2. **Filter executed instances** to find original (non-rollback) step instances
 * 3. **Transform instances into rollback sub-steps** via [buildRollbackSteps]
 * 4. **Determine rollback entry point** based on saga's current execution state
 * 5. **Create continuation** with the appropriate suspension point
 *
 * ## Instance Transformation
 *
 * Each executed step instance becomes one or two rollback sub-steps:
 * ```
 * Normal instance: "PaymentStep" (iteration 0)
 * Becomes:  ["Rollback of PaymentStep[0,] (rolling back child scopes)",
 *            "Rollback of PaymentStep[0,]"]
 *
 * Child failure handler instance: "PaymentStep" (iteration 0, cfh iteration 0)
 * Becomes:  ["Rollback of PaymentStep[0,0] (rolling back child scopes)"]
 * ```
 *
 * See [buildRollbackSteps] for the full transformation logic.
 *
 * ## Sentinel Rollback Steps
 *
 * When there are no rollback steps to execute (e.g., no instances were recorded, or rollback was
 * triggered before any step committed), a "sentinel" rollback step is created via
 * [createSentinelRollbackStep]. This ensures the continuation has a valid step to reference in
 * [SuspensionPoint.AfterLastStep], allowing it to complete immediately.
 *
 * ## Entry Point Logic
 *
 * **Case 1: NotSuspendedYet** - Rollback before any step committed to database. Since no
 * transaction was committed, no messages were emitted, and no child handlers were spawned, there's
 * nothing to roll back. Creates a sentinel step and positions after it so the continuation
 * immediately completes.
 *
 * **Case 2: SuspendedBetweenSteps/LastStepFinished with no rollback steps** - All executed
 * instances were already rollback steps, or there are no original instances to roll back. Creates a
 * sentinel and completes.
 *
 * **Case 3: SuspendedBetweenSteps/LastStepFinished, rollback not yet started** - The
 * [SuspensionState][SuspensionState.SuspendedBetweenSteps] type itself tells us this is a
 * happy-path suspension (as opposed to
 * [SuspendedAfterStepRollback][SuspensionState.SuspendedAfterStepRollback]), meaning we're
 * transitioning from normal execution to rollback for the first time. Starts before the first
 * rollback step.
 *
 * **Case 4: SuspendedAfterStepRollback** - Rollback is already in progress. Finds the current
 * position in the rollback sequence and either completes (if at the last step) or continues to the
 * next rollback sub-step.
 *
 * ## Collision Avoidance
 *
 * The method automatically detects if user-defined steps conflict with generated rollback step
 * names and appends '$' characters until uniqueness is achieved.
 *
 * @param connection Database connection for the continuation's transaction
 * @param coroutineState Current saga state (which step, context, executed instances, etc.)
 * @param scopeCapabilities Provides capabilities for scope operations
 * @return A rollback continuation positioned to resume at the correct rollback sub-step
 * @see buildRollbackSteps for the instance-based step transformation logic
 */
internal fun DistributedCoroutine.buildRollbackPathContinuation(
    connection: Connection,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
): RollbackPathContinuation {
    // Phase 1: Generate unique prefixes/suffixes to avoid conflicts with user-defined step names.
    // If a user step is named "Rollback of SomeStep", we append '$' until it's unique.
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

    // Phase 2: Filter to only original (non-rollback) step instances.
    // Rollback step instances (from a previous partial rollback) are excluded since they
    // don't need their own rollback.
    val originalInstances =
        coroutineState.executedStepInstances.filter { instance ->
            !instance.step.startsWith(rollingBackPrefix)
        }

    // Phase 3: Transform original instances into rollback sub-steps.
    val rollbackSteps =
        buildRollbackSteps(
            rollingBackPrefix,
            rollingBackSuffix,
            scopeCapabilities,
            originalInstances,
        )

    rollbackLogger.debug(
        "Built rollback plan: coroutine='{}', rollbackSteps={}",
        identifier,
        rollbackSteps.size,
    )

    // Helper: creates a sentinel rollback step for edge cases where we need a valid step reference
    // but have nothing to actually roll back. Builds rollback steps for a single synthetic instance
    // and returns the last one (or falls back to the first defined step).
    fun createSentinelRollbackStep(stepName: String): TransactionalStep {
        val syntheticRollbackSteps =
            buildRollbackSteps(
                rollingBackPrefix,
                rollingBackSuffix,
                scopeCapabilities,
                listOf(
                    StepInstance(
                        stepName,
                        ChildFailureHandlerIteration.NoChildFailure,
                        Instant.EPOCH,
                    )
                ),
            )
        return if (syntheticRollbackSteps.isNotEmpty()) {
            syntheticRollbackSteps.last()
        } else {
            steps.first()
        }
    }

    // Phase 4: Determine where to start the continuation based on current saga state.
    fun buildRollbackInitiatedContinuation(completedStep: String): RollbackPathContinuation =
        if (rollbackSteps.isEmpty()) {
            // Case 2: No rollback steps to execute (no original instances to roll back).
            // Create a sentinel and complete immediately.
            val sentinel = createSentinelRollbackStep(completedStep)
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(sentinel),
                this,
                scopeCapabilities,
                stepIteration = 0,
                childFailureHandlerIteration = ChildFailureHandlerIteration.NoChildFailure,
            )
        } else {
            // Case 3: Rollback just initiated. We're transitioning from normal
            // execution to rollback. Start before the first rollback step.
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.BeforeFirstStep(rollbackSteps.first()),
                this,
                scopeCapabilities,
                stepIteration = 0,
                childFailureHandlerIteration = ChildFailureHandlerIteration.NoChildFailure,
            )
        }

    return when (val state = coroutineState.suspensionState) {
        is SuspensionState.NotSuspendedYet -> {
            // Case 1: Rollback before any step committed to database.
            // Since no transaction was committed, no messages were emitted, no child handlers
            // were spawned, and there's nothing to roll back. Create a sentinel and position
            // after it so the continuation immediately completes.
            val sentinel = createSentinelRollbackStep(steps.first().name)
            RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                SuspensionPoint.AfterLastStep(sentinel),
                this,
                scopeCapabilities,
                stepIteration = 0,
                childFailureHandlerIteration = ChildFailureHandlerIteration.NoChildFailure,
            )
        }

        is SuspensionState.SuspendedBetweenSteps -> {
            // Rollback just initiated — the SuspensionState type tells us this is a
            // happy-path suspension, not a rollback suspension.
            buildRollbackInitiatedContinuation(state.completedStep)
        }

        is SuspensionState.LastStepFinished -> {
            // Rollback just initiated — the SuspensionState type tells us this is a
            // happy-path suspension, not a rollback suspension.
            buildRollbackInitiatedContinuation(state.completedStep)
        }

        is SuspensionState.SuspendedAfterStepRollback -> {
            // Case 4: Rollback already in progress.
            // Find our current position in the rollback sequence.
            val lastStepName = state.completedRollbackStep
            val nextStepIdx = rollbackSteps.indexOfFirst { it.name == lastStepName }

            check(nextStepIdx > -1) { "Rollback step $lastStepName was not found" }

            if (nextStepIdx == rollbackSteps.lastIndex) {
                // At the last rollback step — rollback is completing.
                RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    SuspensionPoint.AfterLastStep(rollbackSteps.last()),
                    this,
                    scopeCapabilities,
                    stepIteration = 0,
                    childFailureHandlerIteration = state.childFailureHandlerIteration,
                )
            } else {
                // Rollback continues — move to the next rollback sub-step.
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
                    stepIteration = 0,
                    childFailureHandlerIteration = state.childFailureHandlerIteration,
                )
            }
        }
    }
}

/**
 * Transforms executed step instances into rollback sub-steps for two-phase rollback execution. Each
 * instance becomes one or two rollback sub-steps that execute in reverse chronological order to
 * ensure child-first rollback semantics.
 *
 * This operates on **actual executed instances** rather than step definitions. This is essential
 * for correctly rolling back steps that executed multiple times (via [NextStep.Repeat] or
 * [NextStep.GoTo]), as each execution may have spawned different child scopes.
 *
 * ## Iteration Index Computation
 *
 * The [executedInstances] list arrives in reverse chronological order (created_at DESC from the
 * database). To compute per-step iteration indices for unique naming:
 * 1. Reverse to chronological order
 * 2. For each non-childFailureHandler instance, assign a monotonically increasing index per step
 *    name
 * 3. For childFailureHandler instances, share the iteration index of the most recent
 *    non-childFailureHandler instance of the same step
 * 4. Reverse back to get most-recent-first order for rollback
 *
 * ## Transformation Logic
 *
 * For each executed instance, creates rollback sub-steps depending on whether it was a
 * `handleChildFailures` run:
 *
 * **Normal instance** (childFailureHandlerIteration is
 * [NoChildFailure][ChildFailureHandlerIteration.NoChildFailure]) — generates two steps:
 * 1. **Child Rollback Step**: `"<prefix>StepName[iterIdx,]<suffix>"`
 *     - Emits rollback messages to all child handlers spawned at this instance's
 *       [suspendedAt][StepInstance.suspendedAt] timestamp via
 *       [ScopeCapabilities.emitRollbacksForEmissions]
 * 2. **Self Rollback Step**: `"<prefix>StepName[iterIdx,]"`
 *     - Executes the original step's [TransactionalStep.rollback] method
 *
 * **Child failure handler instance** (childFailureHandlerIteration is
 * [HandlerIteration][ChildFailureHandlerIteration.HandlerIteration]) — generates one step:
 * 1. **Child Rollback Step only**: `"<prefix>StepName[iterIdx,childFailureHandlerIter]<suffix>"`
 *     - Emits rollback messages for child handlers spawned during the failure handling
 *     - No self-rollback step, since the step's own `invoke` was not called
 *
 * @param rollingBackPrefix Unique prefix for rollback step names (e.g., "Rollback of ")
 * @param rollingBackSuffix Unique suffix for child rollback steps (e.g., " (rolling back child
 *   scopes)")
 * @param scopeCapabilities Provides the [ScopeCapabilities.emitRollbacksForEmissions] functionality
 * @param executedInstances Step instances to generate rollback steps for, in reverse chronological
 *   order (most recent first). The rollback steps will execute in this same order, rolling back the
 *   most recent work first.
 * @return List of rollback sub-steps ordered for execution (most recent instance first)
 */
private fun DistributedCoroutine.buildRollbackSteps(
    rollingBackPrefix: String,
    rollingBackSuffix: String,
    scopeCapabilities: ScopeCapabilities,
    executedInstances: List<StepInstance>,
): List<TransactionalStep> {
    // Compute per-step iteration index for unique naming.
    // executedInstances is in reverse chronological order (created_at DESC).
    // For each step, count non-childFailureHandler instances chronologically to get the iteration
    // index.
    // childFailureHandler instances share the iteration index of the most recent
    // non-childFailureHandler instance.
    val instancesWithIterIdx = run {
        val chronological = executedInstances.reversed()
        val stepCounters = mutableMapOf<String, Int>()
        chronological
            .map { instance ->
                val iterIdx =
                    when (instance.childFailureHandlerIteration) {
                        is ChildFailureHandlerIteration.NoChildFailure -> {
                            val idx = stepCounters.getOrDefault(instance.step, 0)
                            stepCounters[instance.step] = idx + 1
                            idx
                        }
                        is ChildFailureHandlerIteration.HandlerIteration -> {
                            // childFailureHandler instance shares iteration with the preceding
                            // non-childFailureHandler instance
                            (stepCounters.getOrDefault(instance.step, 1)) - 1
                        }
                    }
                instance to iterIdx
            }
            .reversed()
    }
    return instancesWithIterIdx.flatMap { (instance, iterationIndex) ->
        val step = steps.first { it.name == instance.step }
        val childFailureHandlerSuffix =
            when (val handlerIteration = instance.childFailureHandlerIteration) {
                is ChildFailureHandlerIteration.NoChildFailure -> ""
                is ChildFailureHandlerIteration.HandlerIteration ->
                    handlerIteration.iteration.toString()
            }
        val instanceSuffix = "[${iterationIndex},${childFailureHandlerSuffix}]"
        // Child rollback step: emits rollback messages to child handlers spawned by this instance.
        val childRollbackStep =
            object : TransactionalStep {
                override val name: String
                    get() = rollingBackPrefix + step.name + instanceSuffix + rollingBackSuffix

                override fun invoke(
                    scope: CooperationScope,
                    message: Message,
                    stepIteration: Int,
                ): NextStep = error("Should never be invoked")

                override fun rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: ChildFailureHandlerIteration,
                ) =
                    scopeCapabilities.emitRollbacksForEmissions(
                        scope,
                        instance.suspendedAt,
                        throwable,
                    )

                override fun handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Throwable,
                    stepIteration: Int,
                    childFailureHandlerIteration: Int,
                    nextStep: NextStep,
                ): NextStep =
                    step.handleChildFailures(
                        scope,
                        message,
                        throwable,
                        stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    )
            }

        if (
            instance.childFailureHandlerIteration is ChildFailureHandlerIteration.HandlerIteration
        ) {
            // Child failure handler instance: only the child rollback step is needed
            // (no self-rollback, since invoke was not called during failure handling).
            listOf(childRollbackStep)
        } else {
            // Normal instance: both child rollback and self rollback steps.
            val selfRollbackStep =
                object : TransactionalStep {
                    override val name: String
                        get() = rollingBackPrefix + step.name + instanceSuffix

                    override fun invoke(
                        scope: CooperationScope,
                        message: Message,
                        stepIteration: Int,
                    ): NextStep = step.invoke(scope, message, stepIteration)

                    override fun rollback(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                        stepIteration: Int,
                        childFailureHandlerIteration: ChildFailureHandlerIteration,
                    ) = step.rollback(scope, message, throwable, 0, childFailureHandlerIteration)

                    override fun handleChildFailures(
                        scope: CooperationScope,
                        message: Message,
                        throwable: Throwable,
                        stepIteration: Int,
                        childFailureHandlerIteration: Int,
                        nextStep: NextStep,
                    ): NextStep =
                        step.handleChildFailures(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep,
                        )
                }
            listOf(childRollbackStep, selfRollbackStep)
        }
    }
}

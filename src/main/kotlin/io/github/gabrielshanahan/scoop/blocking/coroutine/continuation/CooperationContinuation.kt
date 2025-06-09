package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.coroutine.CooperationScope
import io.github.gabrielshanahan.scoop.blocking.coroutine.DistributedCoroutine
import io.github.gabrielshanahan.scoop.blocking.coroutine.TransactionalStep
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.ScopeCapabilities
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier
import java.sql.Connection
import org.postgresql.util.PGobject

/**
 * Since the lifetime of a [CooperationScope] is exactly the same as the lifetime of the (delimited)
 * [Continuation], there's no reason not to use the [Continuation] itself as the [CooperationScope].
 * Continuing the book metaphor from [Continuation], if you think of a delimited continuation as the
 * text of a page, the scope can be thought of as the paper on which it is written. It's two
 * different things, but it makes sense to combine the two and just call the result "a page".
 *
 * If you're familiar with Kotlin coroutines, you'll find something similar was done in their
 * implementation (see [kotlinx.coroutines.AbstractCoroutine])
 */
internal interface CooperationContinuation : Continuation, CooperationScope {

    override val continuation
        get() = this
}

/**
 * Represents the place where the saga execution is suspended.
 *
 * These suspension points correspond to the places when a saga pauses execution and waits for child
 * handlers to complete (or, more specifically, for the
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]
 * to signal a saga is ready to be resumed).
 */
internal sealed interface SuspensionPoint {
    /**
     * The saga hasn't started executing yet - about to run the first step.
     *
     * This occurs either when a new saga is triggered by a message but hasn't executed any steps
     * yet, or when a rollback has just started but hasn't rolled back any steps yet. The
     * continuation will execute the first step (or roll back the last successful step) when
     * resumed.
     */
    data class BeforeFirstStep(val firstStep: TransactionalStep) : SuspensionPoint

    /** The saga is between two steps - previous step finished, next step hasn't started. */
    data class BetweenSteps(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        SuspensionPoint

    /**
     * The saga has completed all its steps - about to finish.
     *
     * This represents a point in the execution of the saga when it has finished executing its final
     * step and is waiting for any final child handlers to complete.
     */
    data class AfterLastStep(val lastStep: TransactionalStep) : SuspensionPoint
}

/**
 * Base implementation of [CooperationContinuation] that provides common continuation functionality.
 *
 * This abstract class handles the core continuation logic that's shared between normal execution
 * (happy path) and rollback execution. It manages:
 * - scope capabilities (mostly provided by [scopeCapabilities])
 * - tracking of various data (emitted messages, the current step, etc.)
 * - resuming execution
 *
 * Subclasses ([HappyPathContinuation] and [RollbackPathContinuation]) provide specific logic for
 * their execution modes. The way things are currently written, we actually do some logic in this
 * class that should conceptually be part of those two specializations (specifically, what part of
 * [TransactionalStep] we invoke in [handleFailuresOrResume]). This is done deliberately, as moving
 * it to the subclasses actually makes the code more messy - try it, and you'll see.
 *
 * @param connection Database connection for the current transaction
 * @param context Cooperation context (deadlines, cancellation tokens, custom data)
 * @param scopeIdentifier The cooperation scope this saga is running in
 * @param suspensionPoint Where in the saga execution this continuation represents
 * @param distributedCoroutine The saga definition being executed
 * @param scopeCapabilities Provides the implementations of capabilities exposed by
 *   [CooperationScope] that require interacting with the database
 */
internal abstract class BaseCooperationContinuation(
    override val connection: Connection,
    override var context: CooperationContext,
    override val scopeIdentifier: CooperationScopeIdentifier.Child,
    private val suspensionPoint: SuspensionPoint,
    protected val distributedCoroutine: DistributedCoroutine,
    private val scopeCapabilities: ScopeCapabilities,
) : CooperationContinuation {

    /**
     * Tracks the current step being executed within this continuation. Since suspension points
     * occur "between steps" (after one step finishes, before the next begins), which one is
     * "current" changes during the continuation's execution.
     *
     * ### Key Decision Logic (see [resumeWith])
     *
     * **For BetweenSteps suspension points:**
     * - **Child failure** (`LastStepResult.Failure`): Handle failures on the **previous step** (the
     *   step that emitted the messages that failed) via [TransactionalStep.handleChildFailures]
     * - **Success** (`SuccessfullyInvoked`/`SuccessfullyRolledBack`): Execute the **next step** in
     *   the saga sequence
     *
     * **Example**: Suspended between Step2 and Step3, child of Step2 failed
     * - `currentStep = previousStep` (Step2)
     * - Calls `Step2.handleChildFailures()` to deal with the child failure
     * - Any messages emitted during `handleChildFailures` are attributed to Step2
     *
     * **Example**: Suspended between Step2 and Step3, Step2 completed successfully
     * - `currentStep = nextStep` (Step3)
     * - Calls `Step3.invoke()` or `Step3.rollback()` depending on execution mode
     * - Any messages emitted are attributed to Step3
     *
     * ## What currentStep influences
     * 1. **Message attribution**: All emitted messages are tagged with
     *    [currentStep.name][TransactionalStep.name] in the database for structured cooperation
     *    lineage tracking (see
     *    [EventLoop.mark][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop.mark])
     * 2. **Rollback events**: ROLLING_BACK/ROLLED_BACK events are attributed to the current step
     *    (see [EventLoop.mark][io.github.gabrielshanahan.scoop.blocking.coroutine.EventLoop.mark])
     * 3. **Logging**: The [continuationIdentifier] includes the step name for debugging/correlation
     * 4. **Child failure handling**: Determines which step's `handleChildFailures` method is called
     *    (see [handleFailuresOrResume])
     */
    private lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, distributedCoroutine.identifier)

    /**
     * Provides SQL that returns exception records when this continuation should give up and fail.
     *
     * This method is used by [giveUpIfNecessary] to check if the continuation should abandon
     * execution due to timeouts, cancellation requests, or other failure conditions. Unlike a
     * simple boolean condition, this returns SQL that **selects exception records** when giving up
     * conditions are met.
     *
     * ## How it works
     *
     * The [seen] parameter is the table alias for the SEEN event record in the SQL query. The
     * returned SQL should be a `SELECT` statement that returns exception records (JSONB format)
     * when the continuation should give up, or no rows when execution should continue normally.
     *
     * ## Return format
     *
     * The SQL should return rows with an `exception` column containing `JSONB` representations of
     * [CooperationFailure][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationFailure]
     * objects. These get processed by
     * [MessageEventRepository.fetchGiveUpExceptions][io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.MessageEventRepository.fetchGiveUpExceptions]
     * and converted to
     * [CooperationException][io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException]s.
     *
     * ## Common give-up scenarios
     * - **Timeouts**: Deadline violations in [CooperationContext] return constructed deadline
     *   exceptions
     * - **Cancellation**: `CANCELLATION_REQUESTED` events return the stored cancellation exception
     *
     * ## Implementation examples
     *
     * See concrete implementations in:
     * - [HappyPathContinuation.giveUpStrategy]: Uses
     *   [BaseEventLoopStrategy.giveUpOnHappyPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.BaseEventLoopStrategy.giveUpOnHappyPath]
     * - [RollbackPathContinuation.giveUpStrategy]: Uses
     *   [BaseEventLoopStrategy.giveUpOnRollbackPath][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.BaseEventLoopStrategy.giveUpOnRollbackPath]
     *
     * For SQL examples, see
     * [cancellationRequested][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.cancellationRequested]
     * and
     * [deadlineMissed][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.deadlineMissed]
     * functions.
     *
     * @param seen SQL alias for the `SEEN` `message_event` record being evaluated
     * @return SQL `SELECT` statement that returns exception `JSONB` records when giving up
     */
    abstract fun giveUpStrategy(seen: String): String

    override fun emitted(message: Message) {
        emittedMessages.add(message)
    }

    override fun launch(
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext?,
    ): Message = scopeCapabilities.launch(this, topic, payload, additionalContext)

    override fun launchOnGlobalScope(
        topic: String,
        payload: PGobject,
        context: CooperationContext?,
    ): CooperationRoot = scopeCapabilities.launchOnGlobalScope(this, topic, payload, context)

    override fun giveUpIfNecessary(): Unit =
        scopeCapabilities.giveUpIfNecessary(this, this::giveUpStrategy)

    /**
     * Resumes execution of this continuation from its suspension point.
     *
     * This is the main entry point for continuation resumption and implements the core structured
     * cooperation logic that determines which step should execute based on:
     * 1. **Where the saga was suspended** ([suspensionPoint])
     * 2. **What happened in the previous execution** ([lastStepResult])
     *
     * ## Labor Division
     * - **[resumeWith]**: Manages [currentStep]
     * - **[resumeCoroutine]**: Provides execution safety wrapper
     * - **[handleFailuresOrResume]**: Dispatches to the appropriate step method (business logic)
     * - **[resume]**: Helper for normal step execution (execution flow control)
     *
     * ## Step Selection Decision Tree
     *
     * **BeforeFirstStep**: Always execute the first step (saga just started or rollback just
     * started)
     *
     * **BetweenSteps + Failure**: Execute the **previous step's** failure handler
     * - The previous step emitted messages that failed, so it handles the failure
     * - Calls `previousStep.handleChildFailures()`
     * - Messages emitted during failure handling are attributed to the previous step
     *
     * **BetweenSteps + Success**: Execute the **next step** normally
     * - Previous step completed successfully, time to advance the saga
     * - Calls `nextStep.invoke()` or `nextStep.rollback()` depending on execution mode
     * - Messages emitted are attributed to the next step
     *
     * **AfterLastStep**: Execute final cleanup on the last step
     * - No more steps to advance to, but may need to handle final child failures
     * - Usually results in saga completion unless final failures occur
     *
     * @param lastStepResult What happened in the previous execution cycle - either success or
     *   failure of child handlers
     * @return [Continuation.ContinuationResult] indicating whether the saga should suspend, has
     *   completed, or failed
     * @see resumeCoroutine For the actual execution logic and exception handling
     * @see currentStep For a detailed explanation of how step attribution works
     */
    override fun resumeWith(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (suspensionPoint) {
            is SuspensionPoint.BeforeFirstStep -> {
                // Starting fresh - execute the first step in the saga
                currentStep = suspensionPoint.firstStep
                resumeCoroutine(lastStepResult)
            }

            is SuspensionPoint.BetweenSteps ->
                when (lastStepResult) {
                    is Continuation.LastStepResult.Failure -> {
                        // Child handlers failed - let the step that emitted them handle the failure
                        currentStep = suspensionPoint.previousStep
                        resumeCoroutine(lastStepResult)
                    }

                    is Continuation.LastStepResult.SuccessfullyInvoked,
                    is Continuation.LastStepResult.SuccessfullyRolledBack -> {
                        // Previous step completed successfully - advance to the next step
                        currentStep = suspensionPoint.nextStep
                        resumeCoroutine(lastStepResult)
                    }
                }

            is SuspensionPoint.AfterLastStep -> {
                // All steps executed - handle any final cleanup or failures
                currentStep = suspensionPoint.lastStep
                resumeCoroutine(lastStepResult)
            }
        }

    /**
     * Executes the actual continuation logic with proper exception handling and give-up checking.
     *
     * This method handles the **execution safety and flow control** aspects of continuation
     * resumption, while delegating the **business logic** to [handleFailuresOrResume]. It also
     * checks if the saga should be given up, before and after the step has been invoked. The
     * give-up checks use the [giveUpStrategy] method to determine if this continuation should
     * abandon execution.
     *
     * @param lastStepResult The result from the previous execution cycle
     * @see giveUpIfNecessary For timeout and cancellation checking logic
     * @see handleFailuresOrResume For the actual step execution dispatch
     */
    fun resumeCoroutine(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult {
        return try {
            // Check if we should abandon execution before doing any work
            giveUpIfNecessary()

            // Execute the step logic and check for give-up conditions that arose during execution
            handleFailuresOrResume(lastStepResult).also { giveUpIfNecessary() }
        } catch (e: Exception) {
            Continuation.ContinuationResult.Failure(e)
        }
    }

    /**
     * Dispatches to the appropriate step method based on the last step result.
     *
     * This method implements the **business logic dispatch** for continuation execution. It
     * determines which method on the [currentStep] should be called based on what happened in the
     * previous execution cycle.
     *
     * The actual execution flow control is handled by [resume], while this method focuses purely on
     * the dispatch logic.
     *
     * ## Dispatch Logic
     *
     * **Failure Result**: Always calls [TransactionalStep.handleChildFailures]
     * - Child handlers failed, so the current step needs to handle those failures
     * - The step can choose to retry, ignore, or propagate the failure
     * - If it doesn't throw, then always results in suspension because failure handling may emit
     *   messages of its own
     * - Messages emitted during failure handling are attributed to [currentStep]
     *
     * **SuccessfullyInvoked Result**: Calls [TransactionalStep.invoke] via [resume]
     * - Previous cycle completed successfully, continue with normal execution
     * - This is the "happy path" forward progress through the saga
     * - May suspend if the step emits messages, or complete if this is the last step
     *
     * **SuccessfullyRolledBack Result**: Calls [TransactionalStep.rollback] via [resume]
     * - Previous rollback cycle completed successfully, continue with rollback execution
     * - This is the compensating action execution during saga rollback
     * - May suspend if rollback emits messages, or complete the rollback sequence
     *
     * The dispatch logic ensures that:
     * 1. **Failure handling happens at the right step**: The step that emitted failing messages is
     *    responsible for handling the failures
     * 2. **Message attribution is correct**: All emitted messages are tagged with the [currentStep]
     *    that was set by [resumeWith]
     *
     * @param lastStepResult What happened in the previous execution cycle
     * @see TransactionalStep.handleChildFailures For failure handling logic
     * @see TransactionalStep.invoke For normal step execution
     * @see TransactionalStep.rollback For compensating action execution
     * @see resume For suspension vs completion decision logic
     */
    private fun handleFailuresOrResume(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure -> {
                // Child handlers failed - let the current step handle the failures
                currentStep.handleChildFailures(
                    this,
                    lastStepResult.message,
                    lastStepResult.throwable,
                )
                // Failure handling always suspends (may have emitted retry messages)
                Continuation.ContinuationResult.Suspend(emittedMessages)
            }

            is Continuation.LastStepResult.SuccessfullyInvoked ->
                // Previous execution succeeded - continue with normal forward execution
                resume { currentStep.invoke(this, lastStepResult.message) }

            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                // Previous rollback succeeded - continue with compensating action execution
                resume {
                    currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
                }
        }

    /**
     * Helper method for normal step execution that determines suspension vs. completion.
     *
     * @param resumeStep Lambda containing the step execution logic ([TransactionalStep.invoke] or
     *   [TransactionalStep.rollback])
     */
    private fun resume(resumeStep: () -> Unit): Continuation.ContinuationResult =
        if (suspensionPoint is SuspensionPoint.AfterLastStep) {
            // All steps completed - saga is done, don't execute the step
            Continuation.ContinuationResult.Success
        } else {
            // More steps remain - execute this step and suspend to wait for children
            resumeStep()
            Continuation.ContinuationResult.Suspend(emittedMessages)
        }
}

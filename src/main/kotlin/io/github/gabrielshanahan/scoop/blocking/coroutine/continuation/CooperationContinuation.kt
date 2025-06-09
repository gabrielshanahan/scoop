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
 * A continuation that also serves as a cooperation scope.
 * 
 * This interface represents a key insight in Scoop's design: the continuation (execution state)
 * and the cooperation scope (execution context) are the same object. This allows the step
 * execution code to access both the resumption logic and the emission capabilities through
 * a single interface.
 * 
 * The relationship can be visualized as:
 * ```
 * CooperationContinuation
 *     ├── Continuation (execution logic)
 *     │   ├── resumeWith()
 *     │   └── continuationIdentifier
 *     └── CooperationScope (execution context)
 *         ├── launch()
 *         ├── context
 *         ├── connection
 *         └── giveUpIfNecessary()
 * ```
 * 
 * This dual nature is what allows saga steps to both execute business logic and emit
 * messages within the same transactional context.
 * 
 * TODO: doc that the continuation is equivalent to the scope (demonstrate graphically?)
 */
internal interface CooperationContinuation : Continuation, CooperationScope {

    override val continuation
        get() = this
}

/**
 * Represents where in the saga execution the continuation is suspended.
 * 
 * These suspension points correspond to the moments when a saga pauses execution
 * and waits for child handlers to complete. Understanding these points is crucial
 * for understanding how continuations are built and resumed.
 */
internal sealed interface SuspensionPoint {
    /**
     * The saga hasn't started executing yet - about to run the first step.
     * 
     * This occurs when a new saga is triggered by a message but hasn't executed
     * any steps yet. The continuation will execute the first step when resumed.
     */
    data class BeforeFirstStep(val firstStep: TransactionalStep) : SuspensionPoint

    /**
     * The saga is between two steps - previous step finished, next step hasn't started.
     * 
     * This is the most common suspension point. The saga has completed a step and
     * emitted some messages, and is now waiting for all handlers of those messages
     * to complete before proceeding to the next step.
     */
    data class BetweenSteps(val previousStep: TransactionalStep, val nextStep: TransactionalStep) :
        SuspensionPoint

    /**
     * The saga has completed all its steps - about to finish.
     * 
     * This occurs when the saga has executed its final step and is waiting for
     * any final child handlers to complete before marking the saga as successfully
     * completed.
     */
    data class AfterLastStep(val lastStep: TransactionalStep) : SuspensionPoint
}

/**
 * Base implementation of [CooperationContinuation] that provides common continuation functionality.
 * 
 * This abstract class handles the core continuation logic that's shared between normal execution
 * (happy path) and rollback execution. It manages:
 * - Step identification and tracking
 * - Message emission during step execution  
 * - Suspension point handling
 * - Cancellation checking
 * 
 * Subclasses ([HappyPathContinuation] and [RollbackPathContinuation]) provide specific logic
 * for their execution modes.
 * 
 * @param connection Database connection for the current transaction
 * @param context Cooperation context (deadlines, cancellation tokens, custom data)
 * @param scopeIdentifier The cooperation scope this saga is running in
 * @param suspensionPoint Where in the saga execution this continuation represents
 * @param distributedCoroutine The saga definition being executed
 * @param scopeCapabilities Handles message emission and cooperation logic
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
     * Tracks the current step being executed.
     * 
     * This is set during [resumeWith] and is used to:
     * - Generate the [continuationIdentifier] for logging and correlation
     * - Ensure that emitted messages and rollback events are correctly attributed to the right step
     * 
     * This ensures that emitted messages and rollback events are correctly attributed to the
     * current step being executed, which is essential for structured cooperation tracking.
     */
    private lateinit var currentStep: TransactionalStep

    override val emittedMessages: MutableList<Message> = mutableListOf()

    override val continuationIdentifier: ContinuationIdentifier
        get() = ContinuationIdentifier(currentStep.name, distributedCoroutine.identifier)

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
    ): CooperationRoot =
        scopeCapabilities.launchOnGlobalScope(this, topic, payload, context)

    override fun giveUpIfNecessary(): Unit =
        scopeCapabilities.giveUpIfNecessary(this, this::giveUpStrategy)

    override fun resumeWith(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (suspensionPoint) {
            is SuspensionPoint.BeforeFirstStep -> {
                currentStep = suspensionPoint.firstStep
                resumeCoroutine(lastStepResult)
            }

            is SuspensionPoint.BetweenSteps ->
                when (lastStepResult) {
                    is Continuation.LastStepResult.Failure -> {
                        currentStep = suspensionPoint.previousStep
                        resumeCoroutine(lastStepResult)
                    }

                    is Continuation.LastStepResult.SuccessfullyInvoked,
                    is Continuation.LastStepResult.SuccessfullyRolledBack -> {
                        currentStep = suspensionPoint.nextStep
                        resumeCoroutine(lastStepResult)
                    }
                }

            is SuspensionPoint.AfterLastStep -> {
                currentStep = suspensionPoint.lastStep
                resumeCoroutine(lastStepResult)
            }
        }

    fun resumeCoroutine(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult {
        return try {
            giveUpIfNecessary()
            handleFailuresOrResume(lastStepResult).also { giveUpIfNecessary() }
        } catch (e: Exception) {
            Continuation.ContinuationResult.Failure(e)
        }
    }

    private fun handleFailuresOrResume(
        lastStepResult: Continuation.LastStepResult
    ): Continuation.ContinuationResult =
        when (lastStepResult) {
            is Continuation.LastStepResult.Failure -> {
                currentStep.handleChildFailures(
                    this,
                    lastStepResult.message,
                    lastStepResult.throwable,
                )
                Continuation.ContinuationResult.Suspend(emittedMessages)
            }

            is Continuation.LastStepResult.SuccessfullyInvoked ->
                resume { currentStep.invoke(this, lastStepResult.message) }

            is Continuation.LastStepResult.SuccessfullyRolledBack ->
                resume {
                    currentStep.rollback(this, lastStepResult.message, lastStepResult.throwable)
                }
        }

    private fun resume(resumeStep: () -> Unit): Continuation.ContinuationResult =
        if (suspensionPoint is SuspensionPoint.AfterLastStep) {
            Continuation.ContinuationResult.Success
        } else {
            resumeStep()
            Continuation.ContinuationResult.Suspend(emittedMessages)
        }
}

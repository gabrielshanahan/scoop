package io.github.gabrielshanahan.scoop.blocking.coroutine.continuation

import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.continuation.ContinuationIdentifier

/**
 * Represents a "slice" of a saga that can be executed - essentially a resumable function.
 * 
 * A [Continuation] is a fundamental concept in computer science that represents a computation
 * that can be paused and resumed. In Scoop, continuations represent the execution state of
 * a saga at a specific suspension point.
 * 
 * ## Suspension Points
 * 
 * **Important**: Suspension points occur just AFTER a step finishes but BEFORE the next
 * step starts. This is crucial for understanding when a saga can be resumed - it's not
 * in the middle of executing a step, but rather between steps.
 * 
 * ## Execution Flow
 * 
 * The EventLoop creates continuations and resumes them with information about what happened
 * in the previous step (success, failure, or rollback completion). The continuation then:
 * 1. Determines what the next step should be
 * 2. Executes that step  
 * 3. Returns a result indicating what should happen next
 * 
 * ## Types of Continuations
 * 
 * There are two main types of continuations in Scoop:
 * - [HappyPathContinuation]: For normal forward execution of saga steps
 * - [RollbackPathContinuation]: For executing compensating actions during rollback
 * 
 * For background on the continuation concept, see the glossary in the README.
 * 
 * **Important**: Suspension points occur just AFTER a step finishes but BEFORE the next
 * step starts. This is crucial for understanding when a saga can be resumed - it's not
 * in the middle of executing a step, but rather between steps.
 */
interface Continuation {

    /**
     * Unique identifier for this continuation instance.
     * 
     * This identifier tracks which specific saga instance and step this continuation represents.
     * It's used for logging, debugging, and correlating continuation executions with database events.
     */
    val continuationIdentifier: ContinuationIdentifier

    /**
     * Resumes execution of the saga with the result from the previous step.
     * 
     * This is the core method that drives saga execution. It receives information about
     * what happened in the previous step and determines what to do next:
     * 
     * - Execute the next step in the saga
     * - Handle failures from child sagas  
     * - Complete the saga successfully
     * - Enter rollback mode
     * 
     * @param lastStepResult What happened in the previous step (success, failure, or rollback)
     * @return What should happen next (suspend, success, or failure)
     */
    fun resumeWith(lastStepResult: LastStepResult): ContinuationResult

    /**
     * Represents the outcome of the previous step execution.
     * 
     * This is passed to [resumeWith] to inform the continuation about what happened
     * in the previous step, allowing it to decide what to do next.
     */
    sealed interface LastStepResult {
        /** The message that was being processed */
        val message: Message

        /**
         * The previous step completed successfully.
         * 
         * This means the step's [TransactionalStep.invoke] method completed without
         * throwing an exception, and any child handlers have finished successfully.
         */
        data class SuccessfullyInvoked(override val message: Message) : LastStepResult

        /**
         * The previous step completed its rollback successfully.
         * 
         * This means the step's [TransactionalStep.rollback] method completed without
         * throwing an exception. The [throwable] is the original exception that caused
         * the rollback to begin.
         */
        data class SuccessfullyRolledBack(override val message: Message, val throwable: Throwable) :
            LastStepResult

        /**
         * The previous step failed (or child handlers failed and weren't handled).
         * 
         * This indicates that either:
         * - The step's [TransactionalStep.invoke] method threw an exception
         * - Child handlers failed and [TransactionalStep.handleChildFailures] re-threw the exception  
         * - The step's [TransactionalStep.rollback] method threw an exception during rollback
         */
        data class Failure(override val message: Message, val throwable: Throwable) :
            LastStepResult
    }

    /**
     * Represents the outcome of resuming the continuation.
     * 
     * This tells the EventLoop what happened when the continuation was resumed
     * and what should happen next.
     */
    sealed interface ContinuationResult {
        /**
         * The saga executed a step and is now suspended, waiting for child handlers to complete.
         * 
         * This is the normal result when a step completes successfully and emits messages.
         * The saga will remain suspended until all handlers of the [emittedMessages] have
         * finished executing, at which point it can be resumed again.
         * 
         * **Note**: EMITTED events are recorded at the end of each step regardless of the
         * step's outcome - this ensures that message emission is always tracked for
         * structured cooperation to work correctly.
         * 
         * @param emittedMessages Messages that were emitted during this step's execution
         * 
         * **Important**: EMITTED events are recorded at the end of each step regardless of the
         * step's outcome. This ensures that message emission is always tracked for structured
         * cooperation to work correctly.
         */
        data class Suspend(val emittedMessages: List<Message>) : ContinuationResult

        /**
         * The saga completed successfully.
         * 
         * This indicates that all steps in the saga have been executed successfully
         * and the saga has reached its natural conclusion. No more processing is needed.
         */
        data object Success : ContinuationResult

        /**
         * The saga failed with an unhandled exception.
         * 
         * This indicates that either:
         * - A step threw an exception that wasn't handled by [TransactionalStep.handleChildFailures]
         * - A rollback operation failed
         * - Some other unrecoverable error occurred
         * 
         * When this result is returned, the EventLoop will initiate rollback processing
         * or mark the saga as failed, depending on the current execution state.
         */
        data class Failure(val exception: Throwable) : ContinuationResult
    }
}

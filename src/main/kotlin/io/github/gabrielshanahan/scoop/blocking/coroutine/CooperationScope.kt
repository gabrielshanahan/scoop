package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import java.sql.Connection
import org.postgresql.util.PGobject

/**
 * The execution environment for a saga step, providing access to message emission and shared context.
 * 
 * A [CooperationScope] represents a "cooperation lineage" - a hierarchical grouping of related saga runs
 * where child runs are consequences of messages emitted by parent runs. This is fundamental to how
 * structured cooperation tracks dependencies between distributed operations.
 * 
 * ## Scope Hierarchy
 * 
 * Think of scopes as "baskets" that group related saga runs:
 * - When a saga reacts to a message, it runs within a cooperation scope
 * - Any messages emitted from that saga create child scopes
 * - This creates a tree structure of parent-child scope relationships
 * 
 * For example:
 * ```
 * Global Scope
 *   └── Account Creation Saga (scope A)
 *       ├── Email Verification Saga (scope A.1) 
 *       └── Billing Setup Saga (scope A.2)
 *           └── Payment Processing Saga (scope A.2.1)
 * ```
 * 
 * ## Scope Persistence
 * 
 * **Important**: The same logical scope persists for the entire saga run, including during rollbacks.
 * However, since Scoop is horizontally scalable, there isn't a single [CooperationScope] instance 
 * that exists from start to finish. Instead, a new instance is created each time the saga resumes
 * execution, but they all represent the same logical scope.
 * 
 * ## Message Emission
 * 
 * - [launch]: Emits a message within the current scope (creates child scope)
 * - [launchOnGlobalScope]: Emits a message on the global scope, breaking cooperation lineage
 * 
 * The choice between these methods determines whether the emitted message's handler will be part
 * of the current cooperation tree or independent of it. See:
 * https://developer.porn/posts/introducing-structured-cooperation/#what-if-i-dont-want-to-cooperate
 * 
 * **Important**: The scope stays the same for the entire coroutine run! Even including ROLLING_BACK
 * phases. While individual [CooperationScope] instances are created each time the saga resumes
 * (for horizontal scalability), they all represent the same logical scope throughout the saga's
 * complete lifecycle from start to finish.
 */
interface CooperationScope {

    /**
     * Unique identifier for this cooperation scope.
     * 
     * This identifier tracks the lineage of message emissions and is used to determine
     * parent-child relationships between saga runs. See [CooperationScopeIdentifier] for details.
     */
    val scopeIdentifier: CooperationScopeIdentifier.Child

    /**
     * Shared context data that flows through the cooperation lineage.
     * 
     * The [CooperationContext] allows sharing data between parent and child sagas, similar to 
     * how reactive context works in reactive streams or CoroutineContext in Kotlin coroutines.
     * Context modifications are persisted and available to child handlers.
     * 
     * See [CooperationContext] for details on available context types (deadlines, cancellation tokens, etc.).
     */
    var context: CooperationContext

    /**
     * The continuation object that represents this saga's current execution state.
     * 
     * In blocking mode, the [CooperationScope] instance is actually the same object as the [Continuation].
     * This relationship is what allows the scope to manage step execution and state persistence.
     */
    val continuation: Continuation

    /**
     * The database connection for this saga's current transaction.
     * 
     * This connection is used for any database operations within the current step. All operations
     * using this connection will be committed or rolled back atomically with message emissions.
     */
    val connection: Connection
    
    /**
     * List of messages emitted during the current step execution.
     * 
     * This is used internally to track what messages have been emitted so far in the current step.
     * These messages will be committed to the message queue when the step completes successfully.
     */
    val emittedMessages: List<Message>

    /**
     * Records that a message has been emitted during this step.
     * 
     * This is called internally by the message emission methods to track which messages
     * have been emitted. This tracking is essential for structured cooperation to work,
     * as it allows the system to know which child handlers need to complete before
     * this saga can proceed to its next step.
     * 
     * @param message The message that was emitted
     */
    fun emitted(message: Message)

    /**
     * Emits a message within the current cooperation scope.
     * 
     * This creates a child cooperation scope for any handlers that process the emitted message.
     * The current saga will suspend after completing its current step and wait for all
     * handlers of this message (and any messages they emit) to complete before proceeding.
     * 
     * This is the primary mechanism for triggering child operations while maintaining
     * structured cooperation guarantees.
     * 
     * @param topic The message topic/queue name
     * @param payload The message payload (must be a PostgreSQL JSON object)
     * @param additionalContext Optional context to merge with the current context for child handlers
     * @return The emitted message with its assigned ID and scope information
     */
    fun launch(
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext? = null,
    ): Message

    /**
     * Emits a message on the global scope, breaking cooperation lineage.
     * 
     * Unlike [launch], this method emits a message that is NOT part of the current
     * cooperation scope. Handlers of this message will run independently, and this
     * saga will NOT wait for them to complete before proceeding.
     * 
     * Use this when you want to trigger "fire-and-forget" operations or when you
     * deliberately want to break the cooperation lineage for decoupling purposes.
     * 
     * See: https://developer.porn/posts/introducing-structured-cooperation/#what-if-i-dont-want-to-cooperate
     * 
     * @param topic The message topic/queue name  
     * @param payload The message payload (must be a PostgreSQL JSON object)
     * @param context Optional context for the independent handler (not merged with current context)
     * @return A cooperation root representing the independent scope
     */
    fun launchOnGlobalScope(
        topic: String,
        payload: PGobject,
        context: CooperationContext? = null,
    ): CooperationRoot

    /**
     * Checks for cancellation requests and throws an exception if the saga should be cancelled.
     * 
     * This method checks the current [CooperationContext] for cancellation tokens and deadlines.
     * If a cancellation has been requested or a deadline has passed, it throws an appropriate
     * exception to terminate the saga's execution.
     * 
     * Call this method periodically in long-running steps to enable responsive cancellation.
     * 
     * TODO: DOC that Definition of this should actually be part of step, since
     *  want to be able to create an uncancellable step. Actually, per-coroutine
     *  should be enough, especially for a POC. IF you need it for a specific step,
     *  you can always create one that just emits a message that's consumed by a
     *  non-cancellable coroutine
     */
    fun giveUpIfNecessary()
}

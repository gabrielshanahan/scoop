package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import java.sql.Connection
import org.postgresql.util.PGobject

/**
 * An object which is bound to a single run of a saga. It's created anew for each step, since each
 * step can run in a different service, but conceptually, it always represents the same thing.
 *
 * A [CooperationScope] is fundamentally identified by a "cooperation lineage", which provides a
 * hierarchical grouping of saga runs. When a saga emits a message using [launch], any handlers that
 * process that message become "children" of the emitting saga, creating a parent-child
 * relationship. The parent saga will suspend execution after completing its current step and wait
 * for all child handlers (and their descendants) to finish before proceeding to its next step (this
 * is governed by the
 * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]).
 * This creates a tree structure where each run represents a node in the tree, and each subtree
 * represents a single scope grouping together a saga run and all the work it triggers through
 * message emission. This hierarchical grouping is what enables us to fulfil structured
 * cooperation's fundamental rule: waiting for children to finish before continuing with the parent.
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
 * **Important**: The same logical scope persists for the entire saga run, including during
 * rollbacks. However, since Scoop is horizontally scalable, there isn't a single [CooperationScope]
 * instance that exists from start to finish. Instead, a new instance is created each time the saga
 * resumes execution, but they all represent the same logical scope.
 *
 * ## Message Emission
 * - [launch]: Emits a message within the current scope (creates child scope)
 * - [launchOnGlobalScope]: Emits a message on the global scope, breaking cooperation lineage
 *
 * The choice between these methods determines whether the emitted message's handler will be part of
 * the current cooperation tree or independent of it. See:
 * https://developer.porn/posts/introducing-structured-cooperation/#what-if-i-dont-want-to-cooperate
 */
interface CooperationScope {

    /**
     * Unique identifier for this cooperation scope.
     *
     * This identifier tracks the lineage of message emissions and is used to determine parent-child
     * relationships between saga runs. See [CooperationScopeIdentifier] for details.
     */
    val scopeIdentifier: CooperationScopeIdentifier.Child

    /**
     * Shared context data that flows through the cooperation lineage.
     *
     * The [CooperationContext] allows sharing data between parent and child sagas, similar to how
     * reactive context works in reactive streams or CoroutineContext in Kotlin coroutines. Context
     * modifications are persisted and available to child handlers.
     *
     * See [CooperationContext] for details on available context types (deadlines, cancellation
     * tokens, etc.).
     */
    var context: CooperationContext

    /**
     * The continuation object that represents this saga's current execution state.
     *
     * In Scoop, the [CooperationScope] instance is actually the same object as the [Continuation] -
     * see
     * [CooperationContinuation][io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.CooperationContinuation]
     * for an explanation why that's the case.
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
     * This is called internally by the message emission methods to track which messages have been
     * emitted. This tracking is essential for structured cooperation to work, as it allows the
     * system to figure out which child handlers need to complete before this saga can proceed to
     * its next step.
     *
     * @param message The message that was emitted
     */
    fun emitted(message: Message)

    /**
     * Emits a message within the current cooperation scope.
     *
     * This creates a child cooperation scope for any handlers that process the emitted message. The
     * current saga will suspend after completing its current step and wait for all handlers of this
     * message (and any other messages emitted) to complete before proceeding.
     *
     * This is the primary mechanism for triggering child operations while maintaining structured
     * cooperation guarantees.
     *
     * @param topic The message topic/queue name
     * @param payload The message payload (must be a PostgreSQL JSON object)
     * @param additionalContext Optional context to merge with the current context for child
     *   handlers
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
     * Unlike [launch], this method emits a message that is NOT part of the current cooperation
     * scope. Handlers of this message will run independently, and this saga will NOT wait for them
     * to complete before proceeding.
     *
     * Use this when you want to trigger "fire-and-forget" operations or when you deliberately want
     * to break the cooperation lineage for decoupling purposes.
     *
     * See:
     * https://developer.porn/posts/introducing-structured-cooperation/#what-if-i-dont-want-to-cooperate
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
     * This method checks the current [CooperationContext] for cancellation tokens and deadlines. If
     * a cancellation has been requested or a deadline has passed, it throws an appropriate
     * exception to terminate the saga's execution.
     *
     * Call this method periodically in long-running steps to enable cooperative cancellation.
     *
     * ## Creating Non-Cancellable Steps
     *
     * To create a step that never gives up (and
     * [never surrenders](https://www.youtube.com/watch?v=FNLUS0o69wQ)), emit a message from that
     * step to a dedicated handler with a custom
     * [EventLoopStrategy][io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy.EventLoopStrategy]:
     * ```kotlin
     * // Custom strategy that ignores cancellation
     * class NonCancellableStrategy(
     *     ignoreOlderThan: OffsetDateTime
     * ) : BaseEventLoopStrategy(ignoreOlderThan) {
     *     override fun giveUpOnHappyPath(seen: String) = "SELECT NULL WHERE FALSE"
     *     override fun giveUpOnRollbackPath(seen: String) = "SELECT NULL WHERE FALSE"
     * }
     *
     * // Register a non-cancellable handler
     * messageQueue.subscribe("critical-operation", saga("critical-handler") {
     *     step { scope, message ->
     *         // This work cannot be cancelled once started
     *         performCriticalOperation()
     *     }
     * }, NonCancellableStrategy(OffsetDateTime.now().minusHours(1)))
     *
     * // Use it from a regular saga
     * step { scope, message ->
     *     scope.launch("critical-operation", message.payload)
     * }
     * ```
     *
     * See
     * [SleepEventLoopStrategy][io.github.gabrielshanahan.scoop.blocking.coroutine.builder.SleepEventLoopStrategy]
     * for another example of a custom strategy that implements specialized logic.
     */
    fun giveUpIfNecessary()
}

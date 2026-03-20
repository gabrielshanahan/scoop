package io.github.gabrielshanahan.scoop.coroutine

import io.github.gabrielshanahan.scoop.JsonbHelper

/**
 * A handler that returns a value to its caller.
 *
 * Actions are handlers whose topics are [ActionTopic]s, which wrap the payload in [ActionInput].
 * This ensures that callers always specify where they want the result stored.
 *
 * ## Type Parameters
 * - `I`: Input payload type - what the action receives
 * - `O`: Output result type - what the action stores in the return value store
 *
 * ## Usage
 *
 * Actions must use [storeActionResult] to store their output, which ensures type safety between the
 * declared output type and what's actually stored.
 *
 * Example:
 * ```kotlin
 * class ReadDirectoryAction(
 *     private val filesystem: IAccessFilesystem,
 *     private val handlerRegistry: HandlerRegistry,
 *     override val jsonbHelper: JsonbHelper,
 * ) : Action<ReadDirectoryInput, ReadDirectoryResult>(ReadDirectoryTopic) {
 *
 *     override fun implementation() = saga(handlerName, handlerRegistry.eventLoopStrategy()) {
 *         step("read-directory") { scope, message ->
 *             val actionInput = jsonbHelper.parseActionInput<ReadDirectoryInput>(message.payload)
 *             val result = filesystem.readDirectory(actionInput.payload)
 *             scope.storeActionResult(actionInput, result)
 *         }
 *     }
 * }
 * ```
 */
abstract class Action<I, O : Any>(topic: ActionTopic<I>) : Handler<ActionInput<I>>(topic) {

    /** JsonbHelper for serializing output to JSONB. Must be provided by subclasses. */
    protected abstract val jsonbHelper: JsonbHelper

    /**
     * Type-safe method to store action output.
     *
     * All actions must use this instead of raw [CooperationScope.storeReturnValue] to ensure the
     * output type matches the declared `O` parameter.
     */
    protected fun CooperationScope.storeActionResult(input: ActionInput<I>, output: O) {
        storeReturnValue(input.returnValueVariableName, jsonbHelper.toPGobject(output))
    }
}

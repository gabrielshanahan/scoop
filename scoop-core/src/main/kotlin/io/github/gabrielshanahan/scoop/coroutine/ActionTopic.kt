package io.github.gabrielshanahan.scoop.coroutine

/**
 * A topic for actions (handlers that return values).
 *
 * ActionTopic wraps the payload type [P] in [ActionInput], ensuring that messages published to this
 * topic always include a return value variable name alongside the action-specific payload.
 *
 * Example:
 * ```kotlin
 * object CallAgentTopic : ActionTopic<CallAgentPayload>()
 * object AskUserTopic : ActionTopic<AskUserPayload>()
 * ```
 */
abstract class ActionTopic<P> : Topic<ActionInput<P>>()

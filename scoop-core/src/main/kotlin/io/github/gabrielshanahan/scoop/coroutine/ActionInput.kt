package io.github.gabrielshanahan.scoop.coroutine

import com.fasterxml.jackson.core.type.TypeReference
import io.github.gabrielshanahan.scoop.JsonbHelper

/**
 * A wrapper for action payloads that includes the return value variable name.
 *
 * Actions receive their input wrapped in this class, which pairs the caller's variable name (where
 * the result should be stored) with the action-specific payload.
 *
 * @param P The type of the action-specific payload
 * @property returnValueVariableName Where the action should store its return value
 * @property payload The action-specific input data
 *
 * Example:
 * ```kotlin
 * // Caller wraps the payload:
 * ActionInput(
 *     returnValueVariableName = AgentResult,
 *     payload = CallAgentPayload(prompt = "Analyze this", context = emptyMap())
 * )
 * ```
 */
data class ActionInput<P>(val returnValueVariableName: VariableName, val payload: P)

/**
 * Parses an [ActionInput] from a PGobject.
 *
 * This extension simplifies parsing action payloads in action implementations:
 * ```kotlin
 * val actionInput = jsonbHelper.parseActionInput<MyPayload>(message.payload)
 * ```
 */
inline fun <reified P> JsonbHelper.parseActionInput(pgObject: Any): ActionInput<P> =
    fromPGobject(pgObject, object : TypeReference<ActionInput<P>>() {})

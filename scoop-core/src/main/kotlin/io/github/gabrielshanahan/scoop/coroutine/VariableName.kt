package io.github.gabrielshanahan.scoop.coroutine

import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * A type-safe identifier for a return value variable name.
 *
 * Each variable name is defined as a singleton object extending this class. The object itself is
 * the identifier - there is no string value to pass. Variable names identify the return value slot
 * that a caller expects from an action.
 *
 * Subclasses must be annotated with `@JsonTypeName` to enable polymorphic JSON serialization.
 *
 * Example:
 * ```kotlin
 * @JsonTypeName("PaymentResult")
 * object PaymentResult : VariableName()
 *
 * @JsonTypeName("AgentOutput")
 * object AgentOutput : VariableName()
 * ```
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "_type")
@Suppress("UnnecessaryAbstractClass")
abstract class VariableName

/**
 * The string representation used for database persistence. Uses the simple class name of the
 * singleton object.
 */
val VariableName.serializedValue: String
    get() = this::class.simpleName ?: error("VariableName must be a named class (not anonymous)")

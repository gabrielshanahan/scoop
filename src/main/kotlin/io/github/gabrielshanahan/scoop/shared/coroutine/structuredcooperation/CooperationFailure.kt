package io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation

import java.util.Objects

private const val NO_MESSAGE = "<no message>"
private const val UNKNOWN_FILENAME = "<unknown filename>"
private const val UNKNOWN_CLASSNAME = "<unknown class>"
private const val UNKNOWN_FUNCTION = "<unknown function>"

/**
 * Language-agnostic representation of a stack trace element
 *
 * Represents a single frame in a stack trace that can be serialized and transmitted across service
 * boundaries between different programming languages and platforms.
 *
 * @param fileName The source file where this frame occurred
 * @param lineNumber The line number within the source file
 * @param className The class name (optional, as not all languages have classes)
 * @param functionName The function/method name (optional, for script toplevel code)
 */
data class StackTraceFrame(
    val fileName: String,
    val lineNumber: Int,
    // Not all languages have classes
    val className: String? = null,
    // Could be the toplevel of a script
    val functionName: String? = null,
)

/**
 * Language-agnostic representation of a distributed exception.
 *
 * This is the core data structure for representing failures that can be propagated across service
 * boundaries in structured cooperation, to preserve stack traces and exception chains across those
 * boundaries.
 *
 * ## Distinction from Other Error Types
 * - **CooperationFailure**: Language-agnostic data structure for cross-service communication
 * - **CooperationException**: JVM-specific translation of CooperationFailure for use in Scoop
 * - **ScoopException**: Implementation-specific exceptions within Scoop itself
 *
 * When a saga step fails and needs to propagate that failure to its parent:
 * 1. Local exception is converted to CooperationFailure using `fromThrowable()`
 * 2. CooperationFailure is stored in message_events table as JSON in `exception` column
 * 3. Parent saga retrieves the CooperationFailure and converts to CooperationException using
 *    `toCooperationException()`
 * 4. Parent receives the exception as if thrown locally, but with full distributed context
 *
 * Exceptions are deliberately not deserialized into known types. You should be very cautious
 * writing any code that depends on the specific type of an exception which might come from a
 * completely different service, and whose name might change due to a simple refactoring. Always
 * deserializing into [CooperationException] forces you to use string comparison on
 * [CooperationException.type] in such a scenario, making the code smell more noticeable.
 *
 * The design enables structured cooperation between services in different languages:
 * - E.g., a Python service can emit CooperationFailure JSON that a Kotlin service understands
 * - Each language provides native exception types that wrap CooperationFailure data
 *
 * @param message Human-readable error description
 * @param type The exception type name (e.g., "com.example.BusinessException")
 * @param source Identifier of the system/handler that threw the exception (includes handler name
 *   and UUID)
 * @param stackTrace List of stack trace frames from the original exception
 * @param causes Chain of underlying failures (similar to Exception.cause and suppressed exceptions)
 */
data class CooperationFailure(
    val message: String,
    val type: String,
    // Identifier of the system which threw the exception
    val source: String,
    val stackTrace: List<StackTraceFrame>,
    val causes: List<CooperationFailure> = emptyList(),
) {
    companion object {

        /**
         * Converts a JVM Throwable to a language-agnostic CooperationFailure
         *
         * This method handles the conversion from JVM-specific exception types to the portable
         * CooperationFailure format that can be transmitted across services.
         *
         * @param throwable The exception to convert
         * @param source Identifier for the system emitting this failure (handler name + UUID)
         * @return CooperationFailure representation suitable for cross-service transmission
         */
        fun fromThrowable(throwable: Throwable, source: String): CooperationFailure {
            val stackTrace =
                throwable.stackTrace.map { element ->
                    StackTraceFrame(
                        fileName = element.fileName ?: UNKNOWN_FILENAME,
                        lineNumber = element.lineNumber,
                        className = element.className.takeIf { it != UNKNOWN_CLASSNAME },
                        functionName = element.methodName.takeIf { it != UNKNOWN_FUNCTION },
                    )
                }

            val causes = buildList {
                throwable.cause?.takeIf { it != throwable }?.also { add(fromThrowable(it, source)) }

                throwable.suppressed.forEach { suppressed ->
                    add(fromThrowable(suppressed, source))
                }
            }

            return if (throwable is CooperationException) {
                CooperationFailure(
                    message =
                        throwable.message.substringAfter(
                            "[${throwable.source}] ${throwable.type}: "
                        ),
                    type = throwable.type,
                    source = throwable.source,
                    stackTrace = stackTrace,
                    causes = causes,
                )
            } else {
                CooperationFailure(
                    message = throwable.message ?: NO_MESSAGE,
                    type = throwable.javaClass.name,
                    source = source,
                    stackTrace = stackTrace,
                    causes = causes,
                )
            }
        }

        /**
         * Converts a CooperationFailure to a CooperationException, so it can be used as a normal
         * JVM exception in the code.
         *
         * @param cooperationFailure The failure data to convert
         * @return CooperationException suitable for throwing in JVM code
         */
        fun toCooperationException(cooperationFailure: CooperationFailure): CooperationException =
            CooperationException(
                message = cooperationFailure.message,
                type = cooperationFailure.type,
                source = cooperationFailure.source,
                stackTraceElements =
                    cooperationFailure.stackTrace
                        .map { element ->
                            StackTraceElement(
                                element.className ?: UNKNOWN_CLASSNAME,
                                element.functionName ?: UNKNOWN_FUNCTION,
                                element.fileName.takeIf { it != UNKNOWN_FILENAME },
                                element.lineNumber,
                            )
                        }
                        .toTypedArray(),
                causes = cooperationFailure.causes.map(::toCooperationException),
            )
    }
}

/**
 * CooperationException - JVM exception representation of [CooperationFailure].
 *
 * This is the JVM-specific exception type used within Scoop to represent failures that originated
 * from other services in a structured cooperation system. It wraps the language-agnostic
 * CooperationFailure data in a standard JVM Exception.
 *
 * ## Distinction from Other Error Types
 * - **CooperationFailure**: Language-agnostic data structure for cross-service communication
 * - **CooperationException**: JVM-specific translation of CooperationFailure for use in Scoop
 * - **ScoopException**: Implementation-specific exceptions within Scoop itself
 *
 * @param message Human-readable error description (prefixed with source and type)
 * @param type Original exception type name from the source service
 * @param source Identifier of the system/handler where the original exception occurred
 * @param stackTraceElements Stack trace from the original exception location
 * @param causes Chain of underlying CooperationExceptions (from cause and suppressed exceptions)
 */
class CooperationException(
    message: String,
    val type: String,
    val source: String,
    stackTraceElements: Array<StackTraceElement>,
    val causes: List<CooperationException>,
) :
    Exception(
        "[$source] $type: ${message.takeIf { it.isNotBlank() } ?: NO_MESSAGE}",
        causes.firstOrNull(),
        true,
        true,
    ) {
    init {
        stackTrace = stackTraceElements
        causes.drop(1).forEach(::addSuppressed)
    }

    // Message is never null
    override val message: String
        get() = super.message!!

    // Cause is always a CooperationException
    override val cause: CooperationException?
        get() = causes.firstOrNull()

    // Compare like a data class
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CooperationException

        if (type != other.type) return false
        if (source != other.source) return false
        if (causes != other.causes) return false
        if (message != other.message) return false
        if (!stackTrace.contentEquals(other.stackTrace)) return false

        return true
    }

    // Hash like a data class
    override fun hashCode() = Objects.hash(type, source, causes, message, stackTrace)
}

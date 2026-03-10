package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.VariableName
import io.github.gabrielshanahan.scoop.coroutine.serializedValue
import java.util.UUID

/**
 * Exception thrown when attempting to store a return value that already exists.
 *
 * This indicates a duplicate storage attempt for the same combination of cooperation lineage,
 * handler, and variable name, which violates the unique constraint on the return_value table.
 */
class ReturnValueAlreadyExistsException(
    cooperationLineage: List<UUID>,
    handlerName: String,
    variableName: VariableName,
) :
    RuntimeException(
        "Return value already exists for lineage ${cooperationLineage.joinToString(".")}, " +
            "handler '$handlerName', variable '${variableName.serializedValue}'"
    )

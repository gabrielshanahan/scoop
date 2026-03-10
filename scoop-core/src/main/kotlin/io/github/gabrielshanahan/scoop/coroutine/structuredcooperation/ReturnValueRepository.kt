package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.VariableName
import io.github.gabrielshanahan.scoop.coroutine.serializedValue
import java.sql.Connection
import java.sql.SQLException
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.postgresql.util.PGobject

/**
 * Repository for storing and retrieving return values from actions.
 *
 * Return values are stored with the cooperation lineage, handler name, and variable name as the
 * composite key, allowing parent sagas to retrieve results from their child actions.
 */
class ReturnValueRepository(private val fluentJdbc: FluentJdbc) {

    /**
     * Stores a return value for the given cooperation lineage, handler, and variable name.
     *
     * @param connection Database connection (from the current transaction)
     * @param cooperationLineage The cooperation lineage identifying the scope
     * @param handlerName The name of the handler storing this return value
     * @param variableName Caller-provided identifier for this return value
     * @param value The return value data as a JSONB object
     * @throws ReturnValueAlreadyExistsException if a return value already exists for this tuple
     */
    fun storeReturnValue(
        connection: Connection,
        cooperationLineage: List<UUID>,
        handlerName: String,
        variableName: VariableName,
        value: PGobject,
    ) {
        val lineageArray = connection.createArrayOf("uuid", cooperationLineage.toTypedArray())

        try {
            fluentJdbc
                .queryOn(connection)
                .update(
                    """
                    INSERT INTO return_value (cooperation_lineage, handler_name, variable_name, value)
                    VALUES (:lineage, :handlerName, :variableName, :value)
                    """
                        .trimIndent()
                )
                .namedParam("lineage", lineageArray)
                .namedParam("handlerName", handlerName)
                .namedParam("variableName", variableName.serializedValue)
                .namedParam("value", value)
                .run()
        } catch (e: SQLException) {
            // Check if this is a unique constraint violation
            if (
                e.message?.contains("unique_return_value_per_lineage_handler_variable") == true ||
                    e.cause
                        ?.message
                        ?.contains("unique_return_value_per_lineage_handler_variable") == true
            ) {
                throw ReturnValueAlreadyExistsException(
                    cooperationLineage,
                    handlerName,
                    variableName,
                )
            }
            throw e
        }
    }

    /**
     * Retrieves all return values from direct children for the given variable name.
     *
     * Direct children are those whose cooperation lineage is exactly one element longer than the
     * parent's lineage and has the parent's lineage as a prefix.
     *
     * @param connection Database connection
     * @param parentLineage The cooperation lineage of the parent scope
     * @param variableName The identifier for the return values
     * @return Map of handler name to return value, empty if no return values found
     */
    fun getReturnValues(
        connection: Connection,
        parentLineage: List<UUID>,
        variableName: VariableName,
    ): Map<String, PGobject> {
        val lineageArray = connection.createArrayOf("uuid", parentLineage.toTypedArray())
        val childCardinality = parentLineage.size + 1

        return fluentJdbc
            .queryOn(connection)
            .select(
                """
                SELECT handler_name, value FROM return_value
                WHERE variable_name = :variableName
                  AND :parentLineage <@ cooperation_lineage
                  AND cardinality(cooperation_lineage) = :childCardinality
                """
                    .trimIndent()
            )
            .namedParam("variableName", variableName.serializedValue)
            .namedParam("parentLineage", lineageArray)
            .namedParam("childCardinality", childCardinality)
            .listResult { resultSet ->
                val handlerName = resultSet.getString("handler_name")
                val value = resultSet.getObject("value") as PGobject
                handlerName to value
            }
            .toMap()
    }

    /**
     * Retrieves a specific return value from a direct child by handler name.
     *
     * @param connection Database connection
     * @param parentLineage The cooperation lineage of the parent scope
     * @param variableName The identifier for the return value
     * @param handlerName The name of the specific handler
     * @return The return value as a JSONB object, or null if not found
     */
    fun getReturnValue(
        connection: Connection,
        parentLineage: List<UUID>,
        variableName: VariableName,
        handlerName: String,
    ): PGobject? {
        val lineageArray = connection.createArrayOf("uuid", parentLineage.toTypedArray())
        val childCardinality = parentLineage.size + 1

        return fluentJdbc
            .queryOn(connection)
            .select(
                """
                SELECT value FROM return_value
                WHERE variable_name = :variableName
                  AND handler_name = :handlerName
                  AND :parentLineage <@ cooperation_lineage
                  AND cardinality(cooperation_lineage) = :childCardinality
                """
                    .trimIndent()
            )
            .namedParam("variableName", variableName.serializedValue)
            .namedParam("handlerName", handlerName)
            .namedParam("parentLineage", lineageArray)
            .namedParam("childCardinality", childCardinality)
            .firstResult { resultSet -> resultSet.getObject("value") as PGobject }
            .orElse(null)
    }
}

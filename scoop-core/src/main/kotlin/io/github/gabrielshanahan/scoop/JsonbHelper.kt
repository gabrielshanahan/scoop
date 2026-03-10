package io.github.gabrielshanahan.scoop

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.postgresql.util.PGobject

/**
 * Helper for converting between Kotlin/Java objects and PostgreSQL JSONB columns.
 *
 * Wraps Jackson's [ObjectMapper] to serialize objects into [PGobject] instances (for writes) and
 * deserialize them back (for reads). Reified overloads are provided for common collection types so
 * callers don't need to supply explicit [TypeReference]s.
 */
class JsonbHelper(private val objectMapper: ObjectMapper) {

    /** Serializes [value] to a [PGobject] with type `jsonb`. */
    fun toPGobject(value: Any): PGobject =
        PGobject().apply {
            type = "jsonb"
            this.value = objectMapper.writeValueAsString(value)
        }

    /** Deserializes a [PGobject] into an instance of [clazz]. */
    fun <T> fromPGobject(pgObject: Any, clazz: Class<T>): T =
        objectMapper.readValue((pgObject as PGobject).value, clazz)

    /** Deserializes a [PGobject] using a Jackson [TypeReference] for generic types. */
    fun <T> fromPGobject(pgObject: Any, typeReference: TypeReference<T>): T =
        objectMapper.readValue((pgObject as PGobject).value, typeReference)

    final inline fun <reified T> fromPGobject(pgObject: Any): T =
        fromPGobject(pgObject, T::class.java)

    final inline fun <reified T> fromPGobjectToList(pgObject: Any): List<T> =
        fromPGobject(pgObject, object : TypeReference<List<T>>() {})

    final inline fun <reified T> fromPGobjectToSet(pgObject: Any): Set<T> =
        fromPGobject(pgObject, object : TypeReference<Set<T>>() {})

    final inline fun <reified K, reified V> fromPGobjectToMap(pgObject: Any): Map<K, V> =
        fromPGobject(pgObject, object : TypeReference<Map<K, V>>() {})
}

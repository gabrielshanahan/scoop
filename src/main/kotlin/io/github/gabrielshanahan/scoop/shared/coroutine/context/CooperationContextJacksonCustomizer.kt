package io.github.gabrielshanahan.scoop.shared.coroutine.context

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton

/**
 * CooperationContextJacksonCustomizer - Custom Jackson serialization for lazy deserialization
 *
 * This customizer implements the Jackson plumbing required to achieve lazy deserialization of
 * [CooperationContext] across distributed services.
 *
 * ## Why This Exists
 *
 * Standard Jackson serialization would eagerly deserialize all context elements when reading from
 * JSON, breaking the lazy deserialization performance optimization that's central to
 * [CooperationContext]. This customizer ensures that:
 * 1. **All context data remains as JSON strings** until explicitly accessed
 * 2. **Unknown context elements from other services** are preserved without deserialization
 *    attempts
 * 3. **Known context elements** are only deserialized when accessed via their
 *    [MappedKey][io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedKey]
 *
 * ## Serialization Strategy
 *
 * **Output Format**: Context is serialized as a JSON object where keys are element names and values
 * are the serialized element data:
 * ```json
 * {
 *   "MyCustomData": {"userId": "123", "sessionId": "abc"},
 *   "SomeOtherServiceData": {"unknownField": "value"}
 * }
 * ```
 *
 * **Serialization Process**:
 * - [OpaqueElement][io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.OpaqueElement]:
 *   Write raw JSON directly (no re-serialization)
 * - [MappedElement][io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext.MappedElement]:
 *   Use original Jackson serializer for the typed object
 * - Composite contexts: Recursively serialize all elements
 *
 * ## Deserialization Strategy
 *
 * **Input Processing**: All JSON values are read as strings and stored in
 * [CooperationContextMap.serializedMap] without any deserialization attempts. In effect, only a
 * single level of the JSON is deserialized. This preserves unknown elements and enables lazy
 * loading.
 *
 * **The [CooperationContextDeserializer.readAsString] Method**: This method manually reconstructs
 * JSON strings from Jackson's token stream, handling all JSON constructs (objects, arrays,
 * primitives) to preserve the exact JSON representation for later lazy deserialization.
 *
 * ## Integration Points
 * - **Quarkus Integration**: Registered as [ObjectMapperCustomizer] for automatic setup
 * - **Serializer Caching**: The [oldSerializers] map stores the original Jackson serializers before
 *   our customization is applied. This is the simplest way to delegate back to whatever the
 *   "standard" Jackson serialization behavior would be for each specific type
 * - **ObjectMapper Injection**: Passes ObjectMapper to [CooperationContextMap] for lazy
 *   deserialization
 */
@Singleton
class CooperationContextJacksonCustomizer : ObjectMapperCustomizer {
    override fun customize(objectMapper: ObjectMapper) {
        val module =
            SimpleModule("CooperationContextModule").apply {
                setSerializerModifier(
                    object : BeanSerializerModifier() {
                        override fun modifySerializer(
                            config: SerializationConfig,
                            beanDesc: BeanDescription,
                            serializer: JsonSerializer<*>,
                        ): JsonSerializer<*> {
                            if (
                                CooperationContext::class.java.isAssignableFrom(beanDesc.beanClass)
                            ) {
                                oldSerializers.put(beanDesc.beanClass, serializer)
                                return CooperationContextSerializer()
                            }
                            return serializer
                        }
                    }
                )

                addDeserializer(
                    CooperationContext::class.java,
                    CooperationContextDeserializer(objectMapper),
                )
            }

        objectMapper.registerModule(module)
    }

    companion object {
        val oldSerializers = mutableMapOf<Class<*>, JsonSerializer<*>>()
    }

    /** Custom serializer that writes CooperationContext as a JSON object. */
    private class CooperationContextSerializer :
        StdSerializer<CooperationContext>(CooperationContext::class.java) {
        override fun serialize(
            value: CooperationContext,
            gen: JsonGenerator,
            provider: SerializerProvider,
        ) {
            gen.writeStartObject()
            serializeCooperationContext(value, gen, provider)
            gen.writeEndObject()
        }

        /**
         * Recursively serializes context elements with different strategies based on type.
         * - OpaqueElement: Writes raw JSON directly to preserve unknown service data
         * - MappedElement: Uses original Jackson serializer for type-safe serialization
         * - Composite contexts: Recursively processes all contained elements
         */
        private fun serializeCooperationContext(
            value: CooperationContext,
            gen: JsonGenerator,
            provider: SerializerProvider,
        ): Unit =
            when (value) {
                is CooperationContext.OpaqueElement -> {
                    gen.writeFieldName(value.key.serializedValue)
                    gen.writeRawValue(value.json)
                }

                is CooperationContext.Element -> {
                    gen.writeFieldName(value.key.serializedValue)
                    @Suppress("UNCHECKED_CAST")
                    val oldSerializer =
                        (oldSerializers[value::class.java]
                            ?: BeanSerializerFactory.instance.createSerializer(
                                provider,
                                provider.config.constructType(value.javaClass),
                            ))
                            as JsonSerializer<CooperationContext.Element>
                    oldSerializer.serialize(value, gen, provider)
                }

                else ->
                    value.fold(Unit) { acc, element ->
                        serializeCooperationContext(element, gen, provider)
                    }
            }
    }

    /**
     * Custom deserializer that reads all JSON values as strings for lazy deserialization.
     *
     * This deserializer is the key to the lazy deserialization strategy - it preserves all context
     * data as JSON strings without attempting to deserialize to typed objects.
     */
    private class CooperationContextDeserializer(val objectMapper: ObjectMapper) :
        StdDeserializer<CooperationContext>(CooperationContext::class.java) {

        override fun deserialize(
            parser: JsonParser,
            ctxt: DeserializationContext,
        ): CooperationContext {
            val result = mutableMapOf<String, String>()

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                result[fieldName] = readAsString(parser).toString()
            }

            return CooperationContextMap(result, mutableMapOf(), objectMapper)
        }

        /**
         * Recursively reads JSON tokens and reconstructs them as a JSON string.
         *
         * This manually walks through Jackson's token stream and reconstructs the exact JSON
         * representation, handling all JSON constructs (objects, arrays, primitives, nulls).
         */
        private fun readAsString(
            parser: JsonParser,
            sb: StringBuilder = StringBuilder(),
        ): StringBuilder {
            when (parser.currentToken()) {
                JsonToken.START_OBJECT -> {
                    sb.append('{')
                    var alreadyWroteElem = false
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (alreadyWroteElem) {
                            sb.append(",")
                        }
                        alreadyWroteElem = true
                        readAsString(parser, sb)
                        parser.nextToken()
                        readAsString(parser, sb)
                    }
                    sb.dropLast(1) // drop trailing comma
                    sb.append('}')
                }
                JsonToken.START_ARRAY -> {
                    sb.append('[')
                    var alreadyWroteElem = false
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        if (alreadyWroteElem) {
                            sb.append(",")
                        }
                        alreadyWroteElem = true
                        readAsString(parser, sb)
                    }
                    sb.append(']')
                }
                JsonToken.VALUE_STRING -> sb.append("\"${parser.text}\"")
                JsonToken.VALUE_NUMBER_INT,
                JsonToken.VALUE_NUMBER_FLOAT -> sb.append(parser.numberValue.toString())
                JsonToken.VALUE_TRUE,
                JsonToken.VALUE_FALSE -> sb.append(parser.booleanValue.toString())
                JsonToken.VALUE_NULL -> {}
                JsonToken.FIELD_NAME -> sb.append("\"${parser.text}\":")
                else -> error("Unsupported token: ${parser.currentToken()}")
            }
            return sb
        }
    }
}

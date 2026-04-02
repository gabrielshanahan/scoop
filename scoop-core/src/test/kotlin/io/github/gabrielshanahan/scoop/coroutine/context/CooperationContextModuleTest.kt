package io.github.gabrielshanahan.scoop.coroutine.context

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

// --- Test context element types ---

data object StringKey : CooperationContext.MappedKey<StringElement>()

data class StringElement(val value: String) : CooperationContext.MappedElement(StringKey)

data object NullableKey : CooperationContext.MappedKey<NullableElement>()

data class NullableElement(val value: String?) : CooperationContext.MappedElement(NullableKey)

data object NumbersKey : CooperationContext.MappedKey<NumbersElement>()

data class NumbersElement(val int: Int, val long: Long, val double: Double, val float: Float) :
    CooperationContext.MappedElement(NumbersKey)

data object BoolKey : CooperationContext.MappedKey<BoolElement>()

data class BoolElement(val flag: Boolean) : CooperationContext.MappedElement(BoolKey)

data object ListKey : CooperationContext.MappedKey<ListElement>()

data class ListElement(val items: List<String>) : CooperationContext.MappedElement(ListKey)

data object NestedKey : CooperationContext.MappedKey<NestedElement>()

data class Inner(val x: Int, val y: String)

data class NestedElement(val inner: Inner) : CooperationContext.MappedElement(NestedKey)

data object DeepKey : CooperationContext.MappedKey<DeepElement>()

data class Level3(val value: String)

data class Level2(val level3: Level3)

data class Level1(val level2: Level2)

data class DeepElement(val level1: Level1) : CooperationContext.MappedElement(DeepKey)

data object MixedListKey : CooperationContext.MappedKey<MixedListElement>()

data class MixedListElement(val items: List<Any>) : CooperationContext.MappedElement(MixedListKey)

data object MapKey : CooperationContext.MappedKey<MapElement>()

data class MapElement(val data: Map<String, Any>) : CooperationContext.MappedElement(MapKey)

data object NullableFieldsKey : CooperationContext.MappedKey<NullableFieldsElement>()

data class NullableFieldsElement(val present: String, val absent: String?, val alsoPresent: Int) :
    CooperationContext.MappedElement(NullableFieldsKey)

class CooperationContextModuleTest {

    private val objectMapper =
        ObjectMapper().apply {
            registerKotlinModule()
            registerModule(CooperationContextModule(this))
        }

    /**
     * Serializes a [CooperationContext], deserializes it back, and re-serializes to verify the JSON
     * round-trips exactly.
     */
    private fun assertRoundTrip(context: CooperationContext) {
        val json = objectMapper.writeValueAsString(context)
        val deserialized = objectMapper.readValue(json, CooperationContext::class.java)
        val roundTripped = objectMapper.writeValueAsString(deserialized)
        assertEquals(json, roundTripped, "JSON changed after round-trip")
    }

    /**
     * Serializes a [CooperationContext], deserializes it, then accesses the element via key to
     * verify typed deserialization still works after a round-trip.
     */
    private inline fun <reified E : CooperationContext.MappedElement> assertTypedRoundTrip(
        context: E,
        key: CooperationContext.MappedKey<E>,
    ): E {
        val json = objectMapper.writeValueAsString(context)
        val deserialized = objectMapper.readValue(json, CooperationContext::class.java)
        val element = deserialized[key]
        assertNotNull(element, "Element should be retrievable after round-trip")
        return element!!
    }

    // --- String escaping (the original bug) ---

    @Nested
    inner class StringEscaping {

        @Test
        fun `tab character`() {
            assertRoundTrip(StringElement("hello\tworld"))
        }

        @Test
        fun `newline character`() {
            assertRoundTrip(StringElement("line1\nline2"))
        }

        @Test
        fun `carriage return`() {
            assertRoundTrip(StringElement("line1\rline2"))
        }

        @Test
        fun `carriage return plus newline`() {
            assertRoundTrip(StringElement("line1\r\nline2"))
        }

        @Test
        fun `escaped double quotes`() {
            assertRoundTrip(StringElement("say \"hello\""))
        }

        @Test
        fun `backslashes`() {
            assertRoundTrip(StringElement("C:\\Users\\test\\file.txt"))
        }

        @Test
        fun `forward slash`() {
            assertRoundTrip(StringElement("path/to/file"))
        }

        @Test
        fun `backspace character`() {
            assertRoundTrip(StringElement("hello\bworld"))
        }

        @Test
        fun `form feed character`() {
            assertRoundTrip(StringElement("hello\u000Cworld"))
        }

        @Test
        fun `null byte`() {
            assertRoundTrip(StringElement("hello\u0000world"))
        }

        @Test
        fun `all JSON escape sequences combined`() {
            assertRoundTrip(StringElement("\"\\/\b\u000C\n\r\t"))
        }

        @Test
        fun `multiple special chars in sequence`() {
            assertRoundTrip(StringElement("\t\t\n\n\"\"\\\\"))
        }

        @Test
        fun `special chars preserve typed value`() {
            val result = assertTypedRoundTrip(StringElement("hello\tworld"), StringKey)
            assertEquals("hello\tworld", result.value)
        }
    }

    // --- Unicode ---

    @Nested
    inner class Unicode {

        @Test
        fun `basic multilingual plane - CJK characters`() {
            assertRoundTrip(StringElement("\u4F60\u597D\u4E16\u754C"))
        }

        @Test
        fun `emoji`() {
            assertRoundTrip(StringElement("\uD83D\uDE00\uD83D\uDE80\uD83C\uDF1F"))
        }

        @Test
        fun `arabic text`() {
            assertRoundTrip(StringElement("\u0645\u0631\u062D\u0628\u0627"))
        }

        @Test
        fun `mixed ascii and unicode`() {
            assertRoundTrip(StringElement("hello \u4E16\u754C world"))
        }

        @Test
        fun `unicode control characters`() {
            assertRoundTrip(StringElement("\u0001\u001F"))
        }

        @Test
        fun `zero-width characters`() {
            assertRoundTrip(StringElement("a\u200Bb\u200Cc\uFEFFd"))
        }
    }

    // --- Empty and boundary values ---

    @Nested
    inner class EmptyAndBoundary {

        @Test
        fun `empty string`() {
            assertRoundTrip(StringElement(""))
        }

        @Test
        fun `string with only spaces`() {
            assertRoundTrip(StringElement("   "))
        }

        @Test
        fun `very long string`() {
            assertRoundTrip(StringElement("a".repeat(10_000)))
        }

        @Test
        fun `long string with special chars throughout`() {
            val s = (1..1000).joinToString("") { "item\t$it\n" }
            assertRoundTrip(StringElement(s))
        }

        @Test
        fun `string that looks like JSON`() {
            assertRoundTrip(StringElement("{\"key\": \"value\"}"))
        }

        @Test
        fun `string that looks like a number`() {
            assertRoundTrip(StringElement("12345"))
        }

        @Test
        fun `string that looks like boolean`() {
            assertRoundTrip(StringElement("true"))
        }

        @Test
        fun `string that looks like null`() {
            assertRoundTrip(StringElement("null"))
        }
    }

    // --- Numeric values ---

    @Nested
    inner class Numbers {

        @Test
        fun `positive integers`() {
            assertRoundTrip(NumbersElement(int = 42, long = 42L, double = 42.0, float = 42.0f))
        }

        @Test
        fun `zero`() {
            assertRoundTrip(NumbersElement(int = 0, long = 0L, double = 0.0, float = 0.0f))
        }

        @Test
        fun `negative numbers`() {
            assertRoundTrip(NumbersElement(int = -42, long = -100L, double = -3.14, float = -2.5f))
        }

        @Test
        fun `large numbers`() {
            assertRoundTrip(
                NumbersElement(
                    int = Int.MAX_VALUE,
                    long = Long.MAX_VALUE,
                    double = 1.7976931348623157E308,
                    float = Float.MAX_VALUE,
                )
            )
        }

        @Test
        fun `min value numbers`() {
            assertRoundTrip(
                NumbersElement(
                    int = Int.MIN_VALUE,
                    long = Long.MIN_VALUE,
                    double = Double.MIN_VALUE,
                    float = Float.MIN_VALUE,
                )
            )
        }
    }

    // --- Booleans ---

    @Nested
    inner class Booleans {

        @Test
        fun `true value`() {
            assertRoundTrip(BoolElement(true))
        }

        @Test
        fun `false value`() {
            assertRoundTrip(BoolElement(false))
        }
    }

    // --- Null handling ---

    @Nested
    inner class Nulls {

        @Test
        fun `null field value`() {
            assertRoundTrip(NullableElement(null))
        }

        @Test
        fun `non-null field value`() {
            assertRoundTrip(NullableElement("present"))
        }

        @Test
        fun `mixed null and non-null fields`() {
            assertRoundTrip(NullableFieldsElement("hello", null, 42))
        }
    }

    // --- Collections ---

    @Nested
    inner class Collections {

        @Test
        fun `empty list`() {
            assertRoundTrip(ListElement(emptyList()))
        }

        @Test
        fun `single item list`() {
            assertRoundTrip(ListElement(listOf("only")))
        }

        @Test
        fun `multi item list`() {
            assertRoundTrip(ListElement(listOf("a", "b", "c")))
        }

        @Test
        fun `list with special characters in items`() {
            assertRoundTrip(ListElement(listOf("tab\there", "newline\nhere", "quote\"here")))
        }

        @Test
        fun `list with empty strings`() {
            assertRoundTrip(ListElement(listOf("", "", "")))
        }

        @Test
        fun `nested list via map`() {
            assertRoundTrip(MapElement(mapOf("nested" to listOf(listOf(1, 2), listOf(3, 4)))))
        }
    }

    // --- Nested objects ---

    @Nested
    inner class NestedObjects {

        @Test
        fun `simple nested object`() {
            assertRoundTrip(NestedElement(Inner(42, "hello")))
        }

        @Test
        fun `nested object with special chars`() {
            assertRoundTrip(NestedElement(Inner(1, "hello\tworld\n\"quoted\"")))
        }

        @Test
        fun `deeply nested objects`() {
            assertRoundTrip(DeepElement(Level1(Level2(Level3("deep value")))))
        }

        @Test
        fun `deeply nested with special chars`() {
            assertRoundTrip(DeepElement(Level1(Level2(Level3("deep\t\"value\"\n")))))
        }
    }

    // --- Map/object with dynamic keys ---

    @Nested
    inner class DynamicMaps {

        @Test
        fun `empty map`() {
            assertRoundTrip(MapElement(emptyMap()))
        }

        @Test
        fun `map with various value types`() {
            assertRoundTrip(
                MapElement(
                    mapOf(
                        "string" to "hello",
                        "number" to 42,
                        "bool" to true,
                        "list" to listOf(1, 2, 3),
                    )
                )
            )
        }

        @Test
        fun `map with nested map`() {
            assertRoundTrip(MapElement(mapOf("outer" to mapOf("inner" to "value"))))
        }
    }

    // --- Opaque elements (unknown context from other services) ---

    @Nested
    inner class OpaqueElements {

        private fun roundTripOpaque(json: String): String {
            val key = CooperationContext.UnmappedKey("TestOpaque")
            val element = CooperationContext.OpaqueElement(key, json)
            val serialized = objectMapper.writeValueAsString(element)
            val deserialized = objectMapper.readValue(serialized, CooperationContext::class.java)
            val opaqueResult = deserialized[key]
            assertNotNull(opaqueResult, "Opaque element should survive round-trip")
            return opaqueResult!!.json
        }

        @Test
        fun `simple object`() {
            val json = """{"key":"value"}"""
            assertEquals(json, roundTripOpaque(json))
        }

        @Test
        fun `object with special chars in values`() {
            val json = """{"key":"hello\tworld"}"""
            assertEquals(json, roundTripOpaque(json))
        }

        @Test
        fun `nested object`() {
            val json = """{"a":{"b":{"c":"deep"}}}"""
            assertEquals(json, roundTripOpaque(json))
        }

        @Test
        fun `array value`() {
            val json = """[1,2,3]"""
            assertEquals(json, roundTripOpaque(json))
        }

        @Test
        fun `complex mixed structure`() {
            val json = """{"arr":[1,"two",true,null,{"nested":true}],"num":42,"bool":false}"""
            assertEquals(json, roundTripOpaque(json))
        }

        @Test
        fun `string with all escape sequences`() {
            val json = """{"v":"tab\there\nnewline\r\n\"quoted\"\\backslash"}"""
            assertEquals(json, roundTripOpaque(json))
        }
    }

    // --- Multiple context elements ---

    @Nested
    inner class MultipleElements {

        @Test
        fun `two elements combined via plus`() {
            val ctx = StringElement("hello\tworld") + BoolElement(true)
            assertRoundTrip(ctx)
        }

        @Test
        fun `three elements combined`() {
            val ctx = StringElement("test\n") + BoolElement(false) + NullableElement(null)
            assertRoundTrip(ctx)
        }

        @Test
        fun `complex element plus simple element`() {
            val ctx = NestedElement(Inner(1, "nested\tvalue")) + ListElement(listOf("a\nb", "c\"d"))
            assertRoundTrip(ctx)
        }
    }

    // --- Double round-trip (serialize -> deserialize -> serialize -> deserialize -> serialize) ---

    @Nested
    inner class DoubleRoundTrip {

        private fun assertDoubleRoundTrip(context: CooperationContext) {
            val json1 = objectMapper.writeValueAsString(context)
            val ctx2 = objectMapper.readValue(json1, CooperationContext::class.java)
            val json2 = objectMapper.writeValueAsString(ctx2)
            val ctx3 = objectMapper.readValue(json2, CooperationContext::class.java)
            val json3 = objectMapper.writeValueAsString(ctx3)
            assertEquals(json1, json2, "First round-trip changed the JSON")
            assertEquals(json2, json3, "Second round-trip changed the JSON")
        }

        @Test
        fun `string with special chars survives double round-trip`() {
            assertDoubleRoundTrip(StringElement("line1\tline2\nline3\\end\"done\""))
        }

        @Test
        fun `complex nested structure survives double round-trip`() {
            assertDoubleRoundTrip(DeepElement(Level1(Level2(Level3("deep\t\"value\"\n\\path")))))
        }

        @Test
        fun `multiple elements survive double round-trip`() {
            val ctx =
                StringElement("a\tb") + BoolElement(true) + ListElement(listOf("x\ny", "z\"w"))
            assertDoubleRoundTrip(ctx)
        }
    }
}

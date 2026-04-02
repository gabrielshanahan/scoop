package io.github.gabrielshanahan.scoop.coroutine.context

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

data object TestKey : CooperationContext.MappedKey<TestElement>()

data class TestElement(val value: String) : CooperationContext.MappedElement(TestKey)

class CooperationContextModuleTest {

    private val objectMapper =
        ObjectMapper().apply {
            registerKotlinModule()
            registerModule(CooperationContextModule(this))
        }

    /**
     * Round-trips a [CooperationContext] through JSON serialization and deserialization, then
     * re-serializes the result to verify the JSON is preserved exactly.
     */
    private fun roundTrip(context: CooperationContext): String {
        val json = objectMapper.writeValueAsString(context)
        val deserialized = objectMapper.readValue(json, CooperationContext::class.java)
        return objectMapper.writeValueAsString(deserialized)
    }

    @Test
    fun `readAsString preserves tab characters in strings`() {
        val context = TestElement("hello\tworld")
        val first = objectMapper.writeValueAsString(context)
        val json = roundTrip(context)
        assertEquals(first, json)
    }

    @Test
    fun `readAsString preserves newline characters in strings`() {
        val context = TestElement("hello\nworld")
        val first = objectMapper.writeValueAsString(context)
        val json = roundTrip(context)
        assertEquals(first, json)
    }

    @Test
    fun `readAsString preserves escaped quotes in strings`() {
        val context = TestElement("hello \"world\"")
        val first = objectMapper.writeValueAsString(context)
        val json = roundTrip(context)
        assertEquals(first, json)
    }

    @Test
    fun `readAsString preserves backslashes in strings`() {
        val context = TestElement("C:\\Users\\test")
        val first = objectMapper.writeValueAsString(context)
        val json = roundTrip(context)
        assertEquals(first, json)
    }
}

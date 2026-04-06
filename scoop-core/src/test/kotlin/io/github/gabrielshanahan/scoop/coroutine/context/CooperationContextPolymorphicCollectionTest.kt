package io.github.gabrielshanahan.scoop.coroutine.context

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CooperationContextPolymorphicCollectionTest {

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes(
        JsonSubTypes.Type(Animal.Dog::class, name = "Dog"),
        JsonSubTypes.Type(Animal.Cat::class, name = "Cat"),
    )
    sealed interface Animal {
        data class Dog(val name: String) : Animal

        data class Cat(val name: String, val livesLeft: Int) : Animal
    }

    data class Zoo(val animals: List<Animal>) : CooperationContext.MappedElement(ZooKey)

    object ZooKey : CooperationContext.MappedKey<Zoo>()

    private fun newMapper(): ObjectMapper =
        ObjectMapper().registerKotlinModule().also {
            it.registerModule(CooperationContextModule(it))
        }

    @Test
    fun `polymorphic list inside MappedElement round-trips through CooperationContext`() {
        val mapper = newMapper()
        val original: CooperationContext =
            Zoo(
                animals =
                    listOf(Animal.Dog(name = "Rex"), Animal.Cat(name = "Whiskers", livesLeft = 9))
            )

        val json = mapper.writeValueAsString(original)

        // Sanity: discriminators must appear in the serialized form.
        assert("\"type\":\"Dog\"" in json) {
            "Expected 'type:\"Dog\"' discriminator in serialized JSON, got: $json"
        }
        assert("\"type\":\"Cat\"" in json) {
            "Expected 'type:\"Cat\"' discriminator in serialized JSON, got: $json"
        }

        // Round-trip: deserialize and lazily access the element.
        val restored = mapper.readValue(json, CooperationContext::class.java)
        val zoo = restored[ZooKey]!!

        assertEquals(original[ZooKey], zoo)
    }
}

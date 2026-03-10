package io.github.gabrielshanahan.scoop.quarkus

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.gabrielshanahan.scoop.coroutine.context.CooperationContextModule
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton

/**
 * Quarkus-specific customizer that registers the core [CooperationContextModule] on the
 * Quarkus-managed [ObjectMapper].
 */
@Singleton
class QuarkusCooperationContextCustomizer : ObjectMapperCustomizer {
    override fun customize(objectMapper: ObjectMapper) {
        objectMapper.registerModule(CooperationContextModule(objectMapper))
    }
}

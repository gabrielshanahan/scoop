package io.github.gabrielshanahan.scoop.messaging

import java.time.OffsetDateTime
import java.util.UUID
import org.postgresql.util.PGobject

data class Message(
    val id: UUID,
    val topic: String,
    val payload: PGobject,
    val createdAt: OffsetDateTime,
)

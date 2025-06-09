package io.github.gabrielshanahan.scoop.blocking.messaging

import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import java.time.ZoneOffset
import java.util.UUID
import org.codejargon.fluentjdbc.api.FluentJdbc
import org.postgresql.util.PGobject

/**
 * MessageRepository - Blocking message persistence and retrieval operations
 *
 * Handles the fundamental database operations for storing and retrieving messages in the structured
 * cooperation system. This component provides the data layer foundation that enables message-based
 * communication between sagas while maintaining transactional consistency.
 *
 * ## Structured Cooperation Role
 * MessageRepository supports structured cooperation by:
 * - Ensuring atomic message persistence within saga transaction boundaries
 * - Providing reliable message retrieval for saga continuation and rollback operations
 * - Maintaining message ordering and consistency required for cooperation lineage tracking
 * - Supporting the append-only message log that enables distributed exception propagation
 *
 * ## Key Concepts
 * - **Message**: Immutable record containing id, topic, payload, and timestamp
 * - **Atomic Operations**: All operations respect transaction boundaries for consistency
 * - **Blocking Patterns**: Uses FluentJDBC for synchronous database operations
 * - **Message Log**: Append-only storage pattern supporting structured cooperation queries
 *
 * ## Usage Patterns
 *
 * ```kotlin
 * // Insert message within saga transaction
 * val message = messageRepository.insertMessage(connection, "order-topic", jsonPayload)
 * println("Published message ${message.id}")
 *
 * // Fetch message for saga continuation
 * val message = messageRepository.fetchMessage(connection, messageId)
 * if (message != null) {
 *     handleMessage(message)
 * }
 * ```
 *
 * ## Integration Points
 * - **PostgresMessageQueue**: Uses this repository for message persistence and queue operations
 * - **EventLoop**: Retrieves messages during saga continuation building
 * - **Message Events**: Message IDs reference entries created by this repository
 * - **Saga Transactions**: All operations occur within saga step transaction boundaries
 *
 * ## Important Behaviors
 * - **Transactional Safety**: Operations respect connection transaction state
 * - **RETURNING Clause**: Insert operations return complete Message with generated fields
 * - **Null Handling**: fetchMessage returns null when message not found
 * - **Error Propagation**: Database errors bubble up as unchecked exceptions
 * - **Timestamp Consistency**: All timestamps stored in UTC for cooperation lineage ordering
 *
 * @see io.github.gabrielshanahan.scoop.blocking.messaging.PostgresMessageQueue
 * @see Message
 */
@ApplicationScoped
class MessageRepository(private val fluentJdbc: FluentJdbc) {

    /**
     * Retrieves a message by its unique identifier from the message table.
     *
     * Used during saga continuation building when the EventLoop needs to reconstruct the original
     * message that triggered a handler. Essential for maintaining message context during structured
     * cooperation execution and rollback scenarios.
     *
     * @param connection The blocking database connection within the current transaction
     * @param messageId The UUID of the message to retrieve
     * @return Message if found, or null if not found
     *
     * ## Usage in Structured Cooperation
     * - **Saga Continuation**: EventLoop fetches original trigger messages to build continuations
     * - **Rollback Context**: Compensating actions may need original message data
     * - **Exception Propagation**: Message details included in distributed stack traces
     * - **Cooperation Lineage**: Message IDs link parent-child relationships in lineage hierarchy
     */
    fun fetchMessage(connection: Connection, messageId: UUID): Message? =
        fluentJdbc
            .queryOn(connection)
            .select("SELECT id, topic, payload, created_at FROM message WHERE id = :messageId")
            .namedParam("messageId", messageId)
            .firstResult { resultSet ->
                Message(
                    id = resultSet.getObject("id", UUID::class.java),
                    topic = resultSet.getString("topic"),
                    payload = resultSet.getObject("payload") as PGobject,
                    createdAt =
                        resultSet
                            .getTimestamp("created_at")
                            .toLocalDateTime()
                            .atOffset(ZoneOffset.UTC),
                )
            }
            .orElse(null)

    /**
     * Persists a new message to the message table and returns the complete message with generated
     * fields.
     *
     * This operation is fundamental to structured cooperation as it creates the persistent message
     * records that enable coordination between saga handlers. The INSERT operation occurs within
     * the saga step's transaction boundary, ensuring atomicity with message event creation.
     *
     * @param connection The blocking database connection within the current transaction
     * @param topic The topic name that determines which handlers will process this message
     * @param payload The PostgreSQL JSON payload containing the message data
     * @return Message containing the complete message with generated UUID and timestamp
     *
     * ## Structured Cooperation Integration
     * - **Transaction Boundary**: Executes within saga step transaction for consistency
     * - **Message Events**: Generated message ID is immediately used to create corresponding
     *   EMITTED event
     * - **Cooperation Lineage**: New message becomes child in cooperation hierarchy
     * - **Rollback Support**: Transaction rollback prevents message persistence if step fails
     * - **Handler Triggering**: Persisted messages are discovered by PostgresMessageQueue polling
     *
     * ## Database Behavior
     * - **Generated Fields**: PostgreSQL generates UUID and timestamp automatically
     * - **RETURNING Clause**: Single query both inserts and retrieves complete record
     * - **Atomic Operation**: Message appears in queue only after transaction commits
     * - **Ordering Guarantee**: created_at timestamp provides message ordering within topic
     *
     * @throws RuntimeException if message insertion fails (wraps database exceptions)
     */
    fun insertMessage(connection: Connection, topic: String, payload: PGobject): Message =
        // For PostgreSQL RETURNING clause, we need to use a regular fluentJdbc.select() query
        fluentJdbc
            .queryOn(connection)
            .select(
                "INSERT INTO message (topic, payload) VALUES (:topic, :payload) RETURNING id, created_at"
            )
            .namedParam("topic", topic)
            .namedParam("payload", payload)
            .singleResult { resultSet ->
                Message(
                    id = resultSet.getObject("id", UUID::class.java),
                    topic = topic,
                    payload = payload,
                    createdAt =
                        resultSet
                            .getTimestamp("created_at")
                            .toLocalDateTime()
                            .atOffset(ZoneOffset.UTC),
                )
            }
}

package io.github.gabrielshanahan.scoop.coroutine.structuredcooperation

import io.github.gabrielshanahan.scoop.coroutine.StructuredCooperationTest
import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.coroutine.eventloop.deadline.rollbackPathTimeout
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.github.gabrielshanahan.scoop.transactional
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Regression tests for latent bugs surfaced by the TypeScript port's zero-flakiness soak campaign
 * (see scoop-ts: DECISIONS.md and test/portregressions/). Each test constructs the exact failure
 * condition deterministically, so a regression fails outright instead of resurfacing as rare test
 * flakiness or, worse, silent production misbehaviour.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LatentBugRegressionTest : StructuredCooperationTest() {

    @Inject internal lateinit var messageEventRepository: MessageEventRepository

    /**
     * The step-window CTEs compare same-lineage events with strict `<` on `created_at`, so an
     * emission and its SUSPENDED mark landing in the same microsecond made the emission vanish from
     * "emitted in latest step" — the parent then resumed without waiting for (or rolling back) its
     * children. Seeding the lineage max AHEAD of the wall clock forces the tie condition: without
     * the GREATEST(+1µs) guard, the next engine insert lands at clock_timestamp(), at or before the
     * seeded row.
     */
    @Test
    fun `engine inserts keep created_at strictly increasing per lineage`() {
        val lineage = listOf(UUID.randomUUID())
        val message = SqlTestUtils(fluentJdbc, jsonbHelper).createSimpleMessage("tie-topic")

        val seededCreatedAt =
            fluentJdbc
                .query()
                .select(
                    """
                    INSERT INTO message_event (message_id, type, cooperation_lineage, created_at)
                    VALUES (:messageId, 'EMITTED', :lineage, clock_timestamp() + interval '200 milliseconds')
                    RETURNING created_at::text AS created_at
                    """
                        .trimIndent()
                )
                .namedParam("messageId", message.id)
                .namedParam("lineage", lineage.toTypedArray())
                .singleResult { it.getString("created_at") }

        fluentJdbc.transactional { connection ->
            messageEventRepository.insertMessageEvent(
                connection,
                message.id,
                "SUSPENDED",
                "tie-handler",
                UUID.randomUUID().toString(),
                "0",
                lineage,
                null,
                null,
                null,
                null,
            )
        }

        val nudged =
            fluentJdbc
                .query()
                .select(
                    """
                    SELECT (created_at = :seeded::timestamptz + interval '1 microsecond') AS nudged
                    FROM message_event
                    WHERE type = 'SUSPENDED' AND cooperation_lineage = :lineage
                    """
                        .trimIndent()
                )
                .namedParam("seeded", seededCreatedAt)
                .namedParam("lineage", lineage.toTypedArray())
                .singleResult { it.getBoolean("nudged") }

        Assertions.assertTrue(
            nudged,
            "engine insert must land exactly 1µs after the lineage max, never tie with it",
        )
    }

    /**
     * The rollback deadline element used to serialize under `RollbackPathDeadlineKey` while the
     * give-up SQL (and the V2 partial index) check `RollbackDeadlineKey` — so rollback-path
     * deadlines were silently never enforced. With an already-expired rollback deadline attached at
     * launch, the first rollback resume must give up: ROLLBACK_FAILED, and no rollback lambda ever
     * runs.
     */
    @Test
    fun `a missed rollback-path deadline produces ROLLBACK_FAILED`() {
        val rollbackRan = AtomicBoolean(false)
        val handler =
            saga("rb-deadline-handler", handlerRegistry.eventLoopStrategy()) {
                step(invoke = { _, _ -> }, rollback = { _, _, _ -> rollbackRan.set(true) })
                step { _, _ -> error("Simulated failure to trigger rollback") }
            }
        val subscription = messageQueue.subscribe("rb-deadline-topic", handler)
        try {
            val root = fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    "rb-deadline-topic",
                    jsonbHelper.toPGobject(mapOf("initial" to "true")),
                    rollbackPathTimeout(Duration.ZERO, "regression test"),
                )
            }

            val deadline = System.currentTimeMillis() + 10_000
            var rollbackFailed = false
            while (System.currentTimeMillis() < deadline && !rollbackFailed) {
                rollbackFailed =
                    fluentJdbc
                        .query()
                        .select(
                            "SELECT count(*) AS cnt FROM message_event " +
                                "WHERE type = 'ROLLBACK_FAILED' AND message_id = :messageId"
                        )
                        .namedParam("messageId", root.message.id)
                        .singleResult { it.getLong("cnt") } > 0
                if (!rollbackFailed) {
                    Thread.sleep(50)
                }
            }
            Assertions.assertTrue(
                rollbackFailed,
                "an expired rollback deadline must produce ROLLBACK_FAILED",
            )
            Assertions.assertFalse(
                rollbackRan.get(),
                "give-up must preempt the rollback path — no rollback lambda may run",
            )

            val exceptions =
                fluentJdbc
                    .query()
                    .select(
                        "SELECT coalesce(string_agg(exception::text, ''), '') AS exceptions " +
                            "FROM message_event " +
                            "WHERE message_id = :messageId AND exception IS NOT NULL"
                    )
                    .namedParam("messageId", root.message.id)
                    .singleResult { it.getString("exceptions") }
            Assertions.assertTrue(
                exceptions.contains("MissedRollbackDeadline"),
                "the failure must be attributed to the missed rollback deadline",
            )
        } finally {
            subscription.close()
        }
    }

    /**
     * The `ignoreOlderThan` cutoff defaults to the client clock, but is compared against
     * database-clock timestamps — with the client clock even slightly ahead, a message launched
     * right after the strategy is built is "older than" the cutoff and its SEEN is never created.
     * The [eventLoopStrategy] overload taking an explicit cutoff lets callers anchor it to the
     * database clock; this exercises that path end-to-end.
     */
    @Test
    fun `eventLoopStrategy accepts a database-anchored ignoreOlderThan cutoff`() {
        val dbNow =
            fluentJdbc.query().select("SELECT clock_timestamp() AS db_now").singleResult {
                it.getObject("db_now", OffsetDateTime::class.java)
            }

        val latch = CountDownLatch(1)
        val handler =
            saga(
                "db-anchored-handler",
                handlerRegistry.eventLoopStrategy(ignoreOlderThan = dbNow),
            ) {
                step { _, _ -> latch.countDown() }
            }
        val subscription = messageQueue.subscribe("db-anchored-topic", handler)
        try {
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    "db-anchored-topic",
                    jsonbHelper.toPGobject(mapOf("initial" to "true")),
                )
            }
            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "a message launched right after strategy creation must be processed when the " +
                    "cutoff is anchored to the database clock",
            )
        } finally {
            subscription.close()
        }
    }

    /**
     * The LISTEN registration behind subscribe() completes asynchronously; a NOTIFY fired in that
     * window used to be lost, leaving the worker to the reconcile safety net (30s by default — the
     * acknowledged source of test flakiness in the README). subscribe() now waits (bounded) for the
     * registration. Sleeping past the startup-armed reconcile window first ensures delivery can
     * only come from the NOTIFY path — with the registration racy, this test would have to wait for
     * the 30s net and fail its 10s latch.
     */
    @Test
    fun `a message published immediately after subscribe is processed without the safety net`() {
        val latch = CountDownLatch(1)
        val handler =
            saga("ready-handler", handlerRegistry.eventLoopStrategy()) {
                step { _, _ -> latch.countDown() }
            }
        val subscription = messageQueue.subscribe("ready-topic", handler)
        try {
            // QUIET_TICKS (3) empty passes at the 50ms tick ≈ 150ms; 500ms is comfortably past.
            Thread.sleep(500)
            fluentJdbc.transactional { connection ->
                messageQueue.launch(
                    connection,
                    "ready-topic",
                    jsonbHelper.toPGobject(mapOf("initial" to "true")),
                )
            }
            Assertions.assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "a message published right after subscribe() must be delivered via NOTIFY, " +
                    "not the reconcile safety net",
            )
        } finally {
            subscription.close()
        }
    }
}

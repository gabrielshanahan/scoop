package io.github.gabrielshanahan.scoop.messaging.shutdownrepro

import io.github.gabrielshanahan.scoop.coroutine.builder.saga
import io.github.gabrielshanahan.scoop.messaging.HandlerRegistry
import io.github.gabrielshanahan.scoop.messaging.PostgresMessageQueue
import io.github.gabrielshanahan.scoop.messaging.Subscription
import io.github.gabrielshanahan.scoop.messaging.eventLoopStrategy
import io.quarkus.runtime.Startup
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject

// Hack-test fixture, not a production component. Mirrors topiari's SagaRegistrar
// so that during Quarkus shutdown, @PreDestroy calls Subscription.close() while
// the periodic-tick executors are still polling Postgres. Under broken scoop,
// close() returns before in-flight ticks finish, so ticks fire during ArC/Agroal
// teardown and log "Error in when ticking" repeatedly — that spam is the symptom
// the recipe in this package is designed to capture.
@Startup
@ApplicationScoped
class ReproSubscriptionRegistrar
@Inject
constructor(
    private val messageQueue: PostgresMessageQueue,
    private val handlerRegistry: HandlerRegistry,
) {
    private val subscriptions: List<Subscription> =
        (0 until SUBSCRIPTION_COUNT).map { i ->
            val topic = "scoop-shutdown-repro-$i"
            messageQueue.subscribe(
                topic,
                saga(topic, handlerRegistry.eventLoopStrategy()) { step { _, _ -> } },
            )
        }

    val subscriptionCount: Int
        get() = subscriptions.size

    @PreDestroy
    fun cleanup() {
        subscriptions.forEach { runCatching { it.close() } }
    }

    companion object {
        const val SUBSCRIPTION_COUNT = 20
    }
}

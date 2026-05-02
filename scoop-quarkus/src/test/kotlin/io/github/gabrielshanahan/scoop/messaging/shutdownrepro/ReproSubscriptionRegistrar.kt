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

// Hack-test fixture, not a production component. Models the typical application-side pattern:
// a CDI bean that registers subscriptions on startup and closes them in @PreDestroy. The
// shutdown contract under test is that close() must drain in-flight ticks before returning, so
// when Quarkus tears down ArC and Agroal in parallel with @PreDestroy, no tick keeps running
// against a half-closed DataSource. Without that contract, the executors keep polling and
// produce the "Error in when ticking" log spam this package's recipe captures.
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

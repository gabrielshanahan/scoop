package io.github.gabrielshanahan.scoop.messaging.shutdownrepro

import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

// Hack-test, not a clean unit test. The work is done by ReproSubscriptionRegistrar
// (eagerly @Startup, leaks subscriptions across the test boundary). This test method
// only exists to make Quarkus boot. The reproduction signal lands in the gradle log
// AFTER the test completes, while Quarkus is shutting down the test container — grep the
// captured log for "Error in when ticking" to see the symptom (zero matches under the fix).
//
// @TestProfile forces Quarkus to use a fresh container for this class (any non-default
// config override does the job), so the subscriptions leaked here and the @PreDestroy
// teardown they trigger don't contaminate the default-profile container shared by
// the rest of scoop-quarkus's test classes.
@QuarkusTest
@TestProfile(ShutdownSpamReproTest.IsolationProfile::class)
class ShutdownSpamReproTest {

    class IsolationProfile : QuarkusTestProfile {
        override fun getConfigOverrides(): Map<String, String> =
            mapOf(
                // Forces fresh Quarkus container so the leaked subscriptions
                // and the DataSource we'll race against don't pollute other classes.
                "scoop.repro.isolation" to "true",
                // Tick as fast as possible so the scheduled-tick path is essentially
                // always running. Maximises the chance that close() lands on top of
                // an in-flight tick, which is the in-flight that races Agroal teardown.
                "scoop.tick-interval-ms" to "1",
            )
    }

    // Direct @Inject forces the registrar to be instantiated; we don't rely on
    // @Startup eagerness, which has historically been flaky for beans declared
    // in src/test/kotlin under @TestProfile-isolated containers.
    @Inject lateinit var registrar: ReproSubscriptionRegistrar

    @Test
    fun `subscriptions leak past test end so quarkus shutdown races scoop ticks`() {
        // Touch the injected registrar so the field is read (some Quarkus injection
        // proxies are only resolved on first access, not on field assignment).
        check(registrar.subscriptionCount == ReproSubscriptionRegistrar.SUBSCRIPTION_COUNT)
        // Sleep briefly so several normal scheduled ticks happen on a healthy
        // DataSource before the test method returns and Quarkus begins teardown.
        Thread.sleep(200)
    }
}

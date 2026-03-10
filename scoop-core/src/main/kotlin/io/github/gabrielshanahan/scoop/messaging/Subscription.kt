package io.github.gabrielshanahan.scoop.messaging

/** A handle to an active message-queue subscription; closing it stops delivery. */
fun interface Subscription : AutoCloseable

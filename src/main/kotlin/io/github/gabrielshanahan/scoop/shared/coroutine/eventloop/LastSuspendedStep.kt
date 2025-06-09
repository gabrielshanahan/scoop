package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

sealed interface LastSuspendedStep {
    data object NotSuspendedYet : LastSuspendedStep

    data class SuspendedAfter(val stepName: String) : LastSuspendedStep
}

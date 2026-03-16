package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier
import io.github.gabrielshanahan.scoop.coroutine.eventloop.ChildFailureHandlerIteration

/**
 * ContinuationId = CoroutineId + StepId + stepIteration + childFailureHandlerIteration.
 *
 * Uniquely identifies a specific continuation within the distributed coroutine execution.
 */
data class ContinuationIdentifier(
    val stepName: String,
    val stepIteration: Int,
    val childFailureHandlerIteration: ChildFailureHandlerIteration,
    val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
)

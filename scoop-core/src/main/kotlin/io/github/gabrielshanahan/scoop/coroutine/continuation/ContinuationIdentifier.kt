package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier

/** ContinuationId = CoroutineId + StepId + iteration + childFailureHandlerIteration. */
data class ContinuationIdentifier(
    val stepName: String,
    val iteration: Int = 0,
    val childFailureHandlerIteration: Int? = null,
    val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
)

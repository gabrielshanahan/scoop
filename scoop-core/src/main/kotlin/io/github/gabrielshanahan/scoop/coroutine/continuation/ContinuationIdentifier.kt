package io.github.gabrielshanahan.scoop.coroutine.continuation

import io.github.gabrielshanahan.scoop.coroutine.DistributedCoroutineIdentifier

/** ContinuationId = CoroutineId + StepId. */
data class ContinuationIdentifier(
    val stepName: String,
    val distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
)

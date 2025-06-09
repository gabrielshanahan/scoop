package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop.strategy

import java.time.OffsetDateTime

/**
 * StandardEventLoopStrategy - Default implementation of structured cooperation execution policy
 *
 * This strategy implements the core structured cooperation rule: sagas suspend execution after
 * emitting messages and don't continue until all handlers of those messages have finished
 * executing. It enforces this by checking that all message emissions have corresponding handler
 * starts before allowing sagas to resume, since the sql in
 * [finalSelect][io.github.gabrielshanahan.scoop.reactive.coroutine.structuredcooperation.finalSelect]
 * takes care of the "everyone who started has finished" part.
 *
 * @param ignoreOlderThan Messages older than this timestamp are ignored during execution
 * @param getTopicsToHandlerNames Function providing handler topology - maps topics to handler names
 */
class StandardEventLoopStrategy(
    ignoreOlderThan: OffsetDateTime,
    val getTopicsToHandlerNames: () -> Map<String, List<String>>,
) : BaseEventLoopStrategy(ignoreOlderThan) {
    override fun resumeHappyPath(
        candidateSeen: String,
        emittedInLatestStep: String,
        childSeens: String,
    ): String =
        allEmissionsHaveCorrespondingContinuationStarts(
            getTopicsToHandlerNames(),
            candidateSeen,
            emittedInLatestStep,
            childSeens,
        )

    override fun resumeRollbackPath(
        candidateSeen: String,
        rollbacksEmittedInLatestStep: String,
        childRollingBacks: String,
    ): String =
        allEmissionsHaveCorrespondingContinuationStarts(
            getTopicsToHandlerNames(),
            candidateSeen,
            rollbacksEmittedInLatestStep,
            childRollingBacks,
        )
}

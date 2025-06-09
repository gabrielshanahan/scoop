package io.github.gabrielshanahan.scoop.shared.coroutine.eventloop

import io.github.gabrielshanahan.scoop.shared.coroutine.ScopeException
import io.github.gabrielshanahan.scoop.shared.coroutine.structuredcooperation.CooperationException

class ChildRolledBackException(causes: List<CooperationException>, step: String? = null) :
    ScopeException(
        step?.let { "Child failure occurred while suspended in step [$it]" },
        causes.first(),
        false,
    ) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

class ChildRollbackFailedException(causes: List<Throwable>, step: String? = null) :
    ScopeException(
        step?.let { "Child rollback failure occurred while suspended in step [$it]" },
        causes.first(),
        false,
    ) {
    init {
        causes.drop(1).forEach { addSuppressed(it) }
    }
}

package io.github.gabrielshanahan.scoop.blocking.coroutine

import io.github.gabrielshanahan.scoop.blocking.coroutine.continuation.Continuation
import io.github.gabrielshanahan.scoop.blocking.coroutine.structuredcooperation.CooperationRoot
import io.github.gabrielshanahan.scoop.blocking.messaging.Message
import io.github.gabrielshanahan.scoop.shared.coroutine.CooperationScopeIdentifier
import io.github.gabrielshanahan.scoop.shared.coroutine.context.CooperationContext
import java.sql.Connection
import org.postgresql.util.PGobject

// TODO: Doc that the scope stays the same for entire coroutine run! Even including ROLLING_BACK
/**
 * Represents a group of saga runs, with the property that all runs except a single one (the parent)
 * are a consequence of a message emitted from the parent - either directly from a step of a parent
 * or indirectly, i.e., from a step of a saga that itself ran because a message was emitted from the
 * parent, and so on.
 *
 * A different, perhaps more succinct, way to phrase this would be to say that any a saga reacts to
 * a message, it does so within a "basket" that groups together that run, and any runs that it triggers.
 * If the message was itself emitted from a saga run which is already in some basket, then the new basket
 * becomes (conceptually, not literally) a child of that basket.
 *
 * From this perspective, it makes sense to talk about [launching][launch] a message on a scope.
 * It also allows us to define a "global scope", which is the toplevel scope. This is where you launch
 * messages when you're not already inside a saga. There are situations where you might want to do that
 * from inside a saga run as well - for instance, [when you want to decouple the current run from whatever reacts to that message](https://developer.porn/posts/introducing-structured-cooperation/#what-if-i-dont-want-to-cooperate),
 * and that's what [launchOnGlobalScope] allows you to do.
 *
 * It should be noted that the entire coroutine run (including possible rollbacks) shares
 * the same scope. That does not mean that there is literally a single instance of [CooperationScope]
 * that exists from start to finish - that's not possible, since Scoop is horizontally scalable, and
 * different steps can be executed by different instances. A new instance of [CooperationScope]
 * is created each time a saga is picked up for execution, but they all represent the
 * same scope.
 */
interface CooperationScope {

    val scopeIdentifier: CooperationScopeIdentifier.Child

    var context: CooperationContext

    val continuation: Continuation

    val connection: Connection
    val emittedMessages: List<Message>

    fun emitted(message: Message)

    fun launch(
        topic: String,
        payload: PGobject,
        additionalContext: CooperationContext? = null,
    ): Message

    fun launchOnGlobalScope(
        topic: String,
        payload: PGobject,
        context: CooperationContext? = null,
    ): CooperationRoot

    // TODO: DOC that Definition of this should actually be part of step, since
    //  want to be able to create an uncancellable step. Actually, per-coroutine
    //  should be enough, especially for a POC. IF you need it for a specific step,
    //  you can always create one that just emits a message that's consumed by a
    //  non-cancellable coroutine
    fun giveUpIfNecessary()
}

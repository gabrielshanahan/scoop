-- Adds columns to message_event that enable loop and GoTo control flow in sagas.
--
-- next_step (INT, nullable):
--   Recorded on SUSPENDED events to indicate which step index the saga should execute next. This
--   is determined by the NextStep returned from invoke (or handleChildFailures):
--     - NextStep.Continue → current index + 1 (or NULL if this was the last step)
--     - NextStep.Repeat   → current index (re-execute the same step)
--     - NextStep.GoTo(n)  → n (jump to step at index n)
--   NULL means the saga should finish and transition to COMMITTED.
--   NOTE: next_step is only meaningful on the happy path. On the rollback path, synthetic rollback
--   step names are not found in the saga's step list, so next_step is always written as NULL.
--   Rollback execution order is determined by step names and executedStepInstances instead.
--   See: NextStep in DistributedCoroutine.kt
--   See: EventLoop.markSuspended in EventLoop.kt
--
-- child_failure_handler_iteration (INT, nullable):
--   Tracks which invocation of handleChildFailures produced a given event. When a step's
--   handleChildFailures callback runs, it may emit messages of its own; if any of those child
--   handlers fail, handleChildFailures is called again with an incremented iteration counter
--   (0, 1, 2, ...). This cycle continues as long as handleChildFailures succeeds but its
--   newly emitted child handlers fail. Each invocation emits its own SUSPENDED event with this
--   counter, allowing the event loop to distinguish events from different handleChildFailures
--   runs. NULL for events not related to child failure handling (i.e., normal invoke/rollback
--   events).
--   See: TransactionalStep.handleChildFailures in DistributedCoroutine.kt
--   See: CooperationContinuation.handleFailuresOrResume in CooperationContinuation.kt

ALTER TABLE message_event ADD COLUMN IF NOT EXISTS next_step INT;
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS child_failure_handler_iteration INT;

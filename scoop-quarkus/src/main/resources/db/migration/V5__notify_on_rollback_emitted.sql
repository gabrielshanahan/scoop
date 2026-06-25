-- Fire a LISTEN/NOTIFY when a ROLLBACK_EMITTED event is inserted, so that rollbacks become
-- event-driven exactly like ordinary emissions.
--
-- Background: ordinary emissions INSERT a row into `message`, whose AFTER INSERT trigger
-- (notify_message_insert, see V1) does pg_notify(topic, message_id). That notification is what lets
-- the event loop reconcile EMITTED→SEEN on demand instead of polling every tick.
--
-- A ROLLBACK_EMITTED event, however, is a `message_event` row that REUSES an existing message_id —
-- it does NOT insert into `message`, so it fires no notification. Without one, the
-- ROLLBACK_EMITTED→ROLLING_BACK reconciliation could only be picked up by the periodic safety-net
-- sweep once reconciliation is gated on notifications. This trigger restores prompt, NOTIFY-driven
-- rollback start by emitting the same (channel = topic, payload = message_id) notification the
-- message trigger does, so the existing per-topic listeners pick it up with no application change.
--
-- The notification only fires for ROLLBACK_EMITTED (a rare event); the constant SEEN / SUSPENDED /
-- COMMITTED / ... churn pays only the cheap WHEN check and never notifies. ROLLING_BACK (which this
-- reconciliation inserts) is not ROLLBACK_EMITTED, so there is no notification loop.

CREATE OR REPLACE FUNCTION notify_rollback_emitted()
    RETURNS TRIGGER AS $$
DECLARE
    v_topic text;
BEGIN
    -- The referenced message always exists and is immutable: ROLLBACK_EMITTED reuses the message_id
    -- of an already-committed emission.
    SELECT topic INTO v_topic FROM message WHERE id = NEW.message_id;
    IF v_topic IS NOT NULL THEN
        PERFORM pg_notify(v_topic, NEW.message_id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER message_event_rollback_emitted_notify_trigger
    AFTER INSERT ON message_event
    FOR EACH ROW
    WHEN (NEW.type = 'ROLLBACK_EMITTED')
    EXECUTE FUNCTION notify_rollback_emitted();

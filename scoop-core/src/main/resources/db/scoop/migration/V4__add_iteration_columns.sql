ALTER TABLE message_event ADD COLUMN iteration INT NOT NULL DEFAULT 0;
ALTER TABLE message_event ADD COLUMN child_failure_handler_iteration INT;

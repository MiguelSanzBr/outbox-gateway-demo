-- Todo UPDATE na outbox soma 1 em version, inclusive o feito na mão. O painel usa pra
-- saber qual de duas mudanças da mesma linha é a mais nova (attempts não serve mais:
-- o release do shutdown e o redrive voltam o attempts).
ALTER TABLE outbox ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

CREATE FUNCTION outbox_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version := OLD.version + 1;
    RETURN NEW;
END $$;
CREATE TRIGGER outbox_version BEFORE UPDATE ON outbox
    FOR EACH ROW EXECUTE FUNCTION outbox_version();

CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox_events', json_build_object(
        'id', NEW.message_id, 'recipient', NEW.recipient, 'status', NEW.status,
        'attempts', NEW.attempts, 'worker', NEW.worker, 'version', NEW.version,
        'last_error', left(NEW.last_error, 200))::text);
    RETURN NULL;
END $$;

-- Registro de domínio: o que o cliente pediu para enviar.
CREATE TABLE IF NOT EXISTS messages (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    recipient       TEXT NOT NULL,
    body            TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox: o que ainda falta entregar ao provedor. Gravada na mesma transação que messages.
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    message_id      BIGINT NOT NULL UNIQUE REFERENCES messages (id),
    recipient       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'sending', 'sent', 'dead')),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token     UUID,
    lease_until     TIMESTAMPTZ,
    last_error      TEXT,
    sent_at         TIMESTAMPTZ
);

-- Linhas prontas para reivindicar, na ordem de chegada.
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox (id) WHERE status = 'pending';
-- "Existe mensagem anterior em aberto para este destinatário?"
CREATE INDEX IF NOT EXISTS outbox_open ON outbox (recipient, id) WHERE status IN ('pending', 'sending');
-- Rede de segurança da ordem: no máximo uma mensagem em voo por destinatário.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_one_inflight ON outbox (recipient) WHERE status = 'sending';
-- Busca do reaper.
CREATE INDEX IF NOT EXISTS outbox_lease ON outbox (lease_until) WHERE status = 'sending';

-- Último worker q reivindicou a linha, pra o painel mostrar quem está com ela.
-- ALTER separado pq o CREATE TABLE IF NOT EXISTS não mexe em banco criado antes dessa coluna.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS worker INT;

-- Toda mudança de estado vira NOTIFY, no formato do domain.Change. Só sai no commit,
-- na ordem dos commits, e se ninguém estiver ouvindo se perde.
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox_events', json_build_object(
        'id', NEW.message_id, 'recipient', NEW.recipient, 'status', NEW.status,
        'attempts', NEW.attempts, 'worker', NEW.worker, 'last_error', left(NEW.last_error, 200))::text);
    RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER outbox_notify AFTER INSERT OR UPDATE ON outbox
    FOR EACH ROW EXECUTE FUNCTION outbox_notify();

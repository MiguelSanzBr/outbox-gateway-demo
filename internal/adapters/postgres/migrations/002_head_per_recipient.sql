-- Só a cabeça de cada destinatário (pending ou sending) pode ser reivindicada; as
-- outras esperam em queued e sobem quando a cabeça vira sent ou dead. O claim deixa
-- de varrer o backlog procurando destinatário livre (era um anti-join por linha).
ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check
    CHECK (status IN ('queued', 'pending', 'sending', 'sent', 'dead'));

-- Linhas de antes: fica de cabeça a sending do destinatário, ou a pending mais velha.
UPDATE outbox o SET status = 'queued'
WHERE o.status = 'pending' AND EXISTS (
    SELECT 1 FROM outbox h
    WHERE h.recipient = o.recipient
      AND (h.status = 'sending' OR (h.status = 'pending' AND h.id < o.id)));

DROP INDEX outbox_ready;
DROP INDEX outbox_open;
DROP INDEX outbox_one_inflight;
-- Claim: a cabeça pronta há mais tempo.
CREATE INDEX outbox_ready ON outbox (next_attempt_at, id) WHERE status = 'pending';
-- Promoção: a próxima da fila do destinatário.
CREATE INDEX outbox_queued ON outbox (recipient, id) WHERE status = 'queued';
-- Rede de segurança da ordem: no máximo uma cabeça por destinatário.
CREATE UNIQUE INDEX outbox_head ON outbox (recipient) WHERE status IN ('pending', 'sending');

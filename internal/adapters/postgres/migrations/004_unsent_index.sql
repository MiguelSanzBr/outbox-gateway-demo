-- /metrics: contagem por estado e a mais antiga ainda não entregue sem varrer as sent,
-- q são quase a tabela toda.
CREATE INDEX outbox_unsent ON outbox (status, message_id) WHERE status <> 'sent';

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// HTTPSender entrega ao provedor (real ou o mock deste repositório).
type HTTPSender struct {
	URL    string // ex.: http://mockprovider:9090/v1/messages
	Client *http.Client
}

func (s HTTPSender) Send(ctx context.Context, c *domain.OutboxEntry) error {
	// Marshal de map[string]string não falha, por isso o erro é ignorado
	payload, _ := json.Marshal(map[string]string{"to": c.Recipient, "body": c.Body})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(payload))
	if err != nil {
		return domain.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", c.IdempotencyKey)
	req.Header.Set("X-Sequence", strconv.FormatInt(c.ID, 10))

	resp, err := s.Client.Do(req)
	if err != nil {
		return err // rede/timeout: resultado incerto, tenta de novo
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))

	err = fmt.Errorf("provedor: HTTP %d", resp.StatusCode)
	switch {
	case resp.StatusCode < 300:
		return nil
	case retryable(resp.StatusCode):
		if d, ok := retryAfter(resp.Header.Get("Retry-After")); ok {
			return domain.RetryAfter(err, d)
		}
		return err
	default:
		return domain.Permanent(err)
	}
}

// retryable: timeout, cedo demais, rate limit e erro do servidor passam se esperar.
func retryable(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooEarly ||
		code == http.StatusTooManyRequests || code >= 500
}

// retryAfter lê o header em segundos ou em data HTTP.
func retryAfter(v string) (time.Duration, bool) {
	if s, err := strconv.Atoi(v); err == nil {
		return time.Duration(s) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t), true
	}
	return 0, false
}

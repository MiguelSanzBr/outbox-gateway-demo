package provider_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/provider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

func TestSenderErrors(t *testing.T) {
	c := &domain.OutboxEntry{ID: 1, IdempotencyKey: "k", Recipient: "ana", Body: "oi"}

	bad := provider.HTTPSender{URL: "http://host com espaço/", Client: http.DefaultClient}
	if err := bad.Send(context.Background(), c); err == nil {
		t.Error("URL inválida não deu erro")
	}

	srv := httptest.NewServer(nil)
	srv.Close()
	down := provider.HTTPSender{URL: srv.URL, Client: srv.Client()}
	if err := down.Send(context.Background(), c); err == nil {
		t.Error("provedor fora do ar não deu erro")
	}
}

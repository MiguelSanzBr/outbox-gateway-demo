package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/httpapi"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// api monta o handler com o caso de uso de verdade em cima do Postgres do teste.
func api(ctx context.Context, store *postgres.Store, demo *app.Demo) http.Handler {
	return httpapi.New(ctx, app.NewService(store, nil, store, app.Config{}), demo, quiet)
}

func TestListRecentMessages(t *testing.T) {
	_, store := testdb.New(t, "t_api_list")
	srv := httptest.NewServer(api(context.Background(), store, nil))
	t.Cleanup(srv.Close)
	for _, k := range []string{"a", "b", "c"} {
		if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: k, Recipient: "ana", Body: k}); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := http.Get(srv.URL + "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var got []struct {
		ID        int64   `json:"id"`
		Recipient string  `json:"recipient"`
		Status    string  `json:"status"`
		Attempts  int     `json:"attempts"`
		Worker    *int    `json:"worker"`
		LastError *string `json:"last_error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(got) != 3 || got[0].ID != 3 || got[2].ID != 1 {
		t.Fatalf("HTTP %d %+v, esperado as 3 da mais nova pra mais velha", resp.StatusCode, got)
	}
	if got[0].Recipient != "ana" || got[0].Status != "pending" || got[0].Worker != nil {
		t.Fatalf("linha: %+v", got[0])
	}
}

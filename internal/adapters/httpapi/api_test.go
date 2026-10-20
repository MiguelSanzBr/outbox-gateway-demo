package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

func TestCreateMessage(t *testing.T) {
	_, store := testdb.New(t, "t_api")
	srv := httptest.NewServer(api(context.Background(), store, nil))
	t.Cleanup(srv.Close)

	post := func(key, body string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	cases := []struct {
		name, key, body string
		want            int
		msg             string // trecho esperado no "error", quando importa
	}{
		{"novo", "a1", `{"recipient":"ana","body":"oi"}`, http.StatusAccepted, ""},
		{"repetido", "a1", `{"recipient":"ana","body":"oi"}`, http.StatusOK, ""},
		{"chave com outro conteúdo", "a1", `{"recipient":"ana","body":"tchau"}`, http.StatusConflict, "reutilizada"},
		{"chave com outro destinatário", "a1", `{"recipient":"bia","body":"oi"}`, http.StatusConflict, "reutilizada"},
		{"sem chave", "", `{"recipient":"ana","body":"oi"}`, http.StatusBadRequest, "Idempotency-Key"},
		{"chave com UTF-8 inválido", "\xff\xfe", `{"recipient":"ana","body":"oi"}`, http.StatusBadRequest, "Idempotency-Key"},
		{"json inválido", "a2", `{`, http.StatusBadRequest, ""},
		{"campo desconhecido", "a2", `{"recipient":"ana","body":"oi","priority":9}`, http.StatusBadRequest, ""},
		{"lixo depois do JSON", "a2", `{"recipient":"ana","body":"oi"} garbage`, http.StatusBadRequest, ""},
		{"dois objetos", "a2", `{"recipient":"ana","body":"oi"}{}`, http.StatusBadRequest, ""},
		{"corpo grande demais", "a2", `{"recipient":"ana","body":"` + strings.Repeat("x", 17<<10) + `"}`, http.StatusRequestEntityTooLarge, ""},
		{"sem destinatário", "a3", `{"body":"oi"}`, http.StatusUnprocessableEntity, "bytes"},
		// NUL passa no JSON (\u0000) mas o Postgres recusa: tem q ser 422, não 500
		{"corpo com NUL", "a4", `{"recipient":"ana","body":"x\u0000y"}`, http.StatusUnprocessableEntity, ""},
	}
	var firstID any
	for _, c := range cases {
		code, out := post(c.key, c.body)
		if code != c.want {
			t.Fatalf("%s: HTTP %d %v, esperado %d", c.name, code, out, c.want)
		}
		if msg, _ := out["error"].(string); c.msg != "" && !strings.Contains(msg, c.msg) {
			t.Fatalf("%s: erro %q, esperado com %q", c.name, msg, c.msg)
		}
		switch c.name {
		case "novo":
			firstID = out["id"]
		case "repetido":
			if out["id"] != firstID {
				t.Fatalf("repetido devolveu id %v, esperado %v", out["id"], firstID)
			}
		}
	}

	resp, err := http.Get(srv.URL + "/v1/messages/1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var st map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&st)
	if resp.StatusCode != http.StatusOK || st["status"] != "pending" {
		t.Fatalf("GET: HTTP %d %v", resp.StatusCode, st)
	}
}

func TestReadEndpointsAndErrors(t *testing.T) {
	_, store := testdb.New(t, "t_api_read")
	ok := httptest.NewServer(api(context.Background(), store, nil))
	t.Cleanup(ok.Close)
	pool, broken := testdb.New(t, "t_api_down")
	pool.Close() // banco fora do ar: a API responde 500 e não vaza o erro do driver
	down := httptest.NewServer(api(context.Background(), broken, nil))
	t.Cleanup(down.Close)

	cases := []struct {
		name, method, url, body string
		want                    int
	}{
		{"healthz", http.MethodGet, ok.URL + "/healthz", "", http.StatusNoContent},
		{"id q não é número", http.MethodGet, ok.URL + "/v1/messages/abc", "", http.StatusBadRequest},
		{"id q não existe", http.MethodGet, ok.URL + "/v1/messages/999", "", http.StatusNotFound},
		{"GET com banco fora", http.MethodGet, down.URL + "/v1/messages/1", "", http.StatusInternalServerError},
		{"lista com banco fora", http.MethodGet, down.URL + "/v1/messages", "", http.StatusInternalServerError},
		{"POST com banco fora", http.MethodPost, down.URL + "/v1/messages", `{"recipient":"ana","body":"oi"}`, http.StatusInternalServerError},
	}
	for _, c := range cases {
		req, _ := http.NewRequest(c.method, c.url, strings.NewReader(c.body))
		req.Header.Set("Idempotency-Key", "k")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		_ = resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: HTTP %d, esperado %d", c.name, resp.StatusCode, c.want)
		}
		if c.want == http.StatusInternalServerError && out["error"] != "erro interno" {
			t.Errorf("%s: vazou detalhe interno: %v", c.name, out)
		}
	}
}

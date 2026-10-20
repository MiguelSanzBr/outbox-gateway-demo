// Package testdb abre um schema isolado no Postgres de DATABASE_URL para testes de integração.
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New recria o schema e devolve um Store migrado. Pula o teste sem DATABASE_URL.
func New(t *testing.T, schema string) (*pgxpool.Pool, *postgres.Store) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL não definido; suba o Postgres com `docker compose up -d postgres`")
	}
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE; CREATE SCHEMA "+ident); err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// application_name = schema pra o teste achar as conexões dele no pg_stat_activity
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	store := postgres.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return pool, store
}

// Broken devolve um Store com o pool já fechado: é o banco fora do ar, sem mock.
func Broken(t *testing.T, schema string) *postgres.Store {
	pool, store := New(t, schema)
	pool.Close()
	return store
}

// Sabotage cria um trigger no schema do teste pra fazer um comando específico falhar dentro da transação.
func Sabotage(t *testing.T, pool *pgxpool.Pool, table, body string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		CREATE FUNCTION sabotage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN `+body+` END $$;
		CREATE TRIGGER sabotage BEFORE INSERT OR UPDATE ON `+pgx.Identifier{table}.Sanitize()+`
		FOR EACH ROW EXECUTE FUNCTION sabotage();`)
	if err != nil {
		t.Fatal(err)
	}
}

// WaitAllSent espera a outbox inteira virar sent, com want linhas.
func WaitAllSent(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var sent, open int
		err := pool.QueryRow(context.Background(), `
			SELECT count(*) FILTER (WHERE status = 'sent'), count(*) FILTER (WHERE status <> 'sent') FROM outbox`).
			Scan(&sent, &open)
		if err != nil {
			t.Fatal(err)
		}
		if sent == want && open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: sent=%d outros=%d, esperado sent=%d", sent, open, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

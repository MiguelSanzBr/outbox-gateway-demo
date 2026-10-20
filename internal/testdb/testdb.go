// Package testdb abre um schema isolado no Postgres de DATABASE_URL para testes de integração.
package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// URL devolve o DATABASE_URL. Sem ele o teste pula, menos no CI, onde pular
// em silêncio deixaria a suíte verde sem testar nada.
func URL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		if os.Getenv("CI") == "true" {
			t.Fatal("DATABASE_URL não definido no CI")
		}
		t.Skip("DATABASE_URL não definido; suba o Postgres com `docker compose up -d postgres`")
	}
	return url
}

// New cria um schema só desse teste (nome + sufixo aleatório, pra duas execuções
// no mesmo banco não apagarem o schema uma da outra) e devolve um Store migrado.
// O schema é apagado no fim do teste.
func New(t *testing.T, name string) (*pgxpool.Pool, *postgres.Store) {
	t.Helper()
	url := URL(t)
	ctx := context.Background()
	schema := fmt.Sprintf("%s_%08x", name, rand.Uint32())
	ident := pgx.Identifier{schema}.Sanitize()
	exec := func(sql string) {
		admin, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE SCHEMA " + ident)
	t.Cleanup(func() { exec("DROP SCHEMA " + ident + " CASCADE") })

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// application_name = schema pra o teste achar as conexões dele no pg_stat_activity (ver AppName)
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

// AppName é o application_name das conexões do pool, único por teste.
func AppName(pool *pgxpool.Pool) string {
	return pool.Config().ConnConfig.RuntimeParams["application_name"]
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

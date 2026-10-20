package postgres_test

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

func versions(t *testing.T, pool *pgxpool.Pool) []int32 {
	t.Helper()
	var v []int32
	if err := pool.QueryRow(context.Background(), `SELECT array_agg(version ORDER BY version) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Réplicas subindo juntas (compose --scale, rolling deploy) migram ao mesmo tempo:
// nenhuma pode falhar no boot e cada versão é aplicada uma vez só.
func TestMigrateConcurrentReplicas(t *testing.T) {
	ctx := context.Background()
	for round := range 5 {
		pool := testdb.Pool(t, "t_migrate")
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Go(func() { errs[i] = postgres.NewStore(pool).Migrate(ctx) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("rodada %d, réplica %d: %v", round, i, err)
			}
		}
		first := versions(t, pool)
		if len(first) == 0 || first[0] != 1 {
			t.Fatalf("schema_migrations = %v", first)
		}
		// boot seguinte não reaplica nada
		if err := postgres.NewStore(pool).Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if again := versions(t, pool); !slices.Equal(again, first) {
			t.Fatalf("segundo boot mudou as versões: %v → %v", first, again)
		}
	}
}

func TestMigrateFailsWithDBDown(t *testing.T) {
	if err := testdb.Broken(t, "t_migrate_down").Migrate(context.Background()); err == nil {
		t.Fatal("Migrate sem erro com banco fora")
	}
}

// Banco criado antes das migrações versionadas (schema.sql rodado a cada boot):
// o 001 é aquele mesmo schema, idempotente, então o primeiro boot novo só registra a versão.
func TestMigrateUpgradesPreVersioningDB(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t, "t_migrate_old")
	old, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(old)); err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewStore(pool).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if v := versions(t, pool); len(v) == 0 || v[0] != 1 {
		t.Fatalf("schema_migrations = %v", v)
	}
}

// Cada passo da migração devolve o erro com o contexto, sem gravar versão pela metade.
func TestMigrateSurfacesStepErrors(t *testing.T) {
	cases := []struct{ name, setup string }{
		// enum não ocupa pg_class: o IF NOT EXISTS não vê e o CREATE TABLE bate no tipo
		{"schema_migrations não dá pra criar", `CREATE TYPE schema_migrations AS ENUM ('x')`},
		{"schema_migrations com outro formato", `CREATE TABLE schema_migrations (version TEXT)`},
		{"migração quebra no meio", `CREATE VIEW messages AS SELECT 1 AS id`},
		{"registro da versão falha", `CREATE TABLE schema_migrations (version INT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now());
			CREATE FUNCTION nope() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$;
			CREATE TRIGGER nope BEFORE INSERT ON schema_migrations FOR EACH ROW EXECUTE FUNCTION nope();`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			pool := testdb.Pool(t, "t_migrate_err")
			if _, err := pool.Exec(ctx, c.setup); err != nil {
				t.Fatal(err)
			}
			if err := postgres.NewStore(pool).Migrate(ctx); err == nil {
				t.Fatal("Migrate sem erro")
			}
		})
	}
}

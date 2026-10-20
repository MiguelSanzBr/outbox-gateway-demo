package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
)

// Migrações versionadas, aplicadas em ordem de nome (001_, 002_, ...). Nunca edite
// uma q já foi pro ar: crie a próxima.
//
//go:embed migrations/*.sql
var migrations embed.FS

func migrationFiles() []string {
	files, _ := fs.Glob(migrations, "migrations/*.sql") // padrão fixo: Glob só falha com padrão inválido
	return files
}

// Migrate aplica as migrações q faltam numa transação só, atrás de um advisory lock:
// réplicas subindo juntas esperam a primeira terminar e depois não acham nada pra fazer.
// DDL só roda uma vez, então o boot não pega lock pesado na outbox à toa.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// A chave do lock é int8, outro espaço de chave do lock por destinatário (dois int4).
	// O lock_timeout vem depois dele: réplica esperando a outra migrar espera o quanto
	// precisar, mas se alguma query longa segurar a outbox a migração desiste em vez de
	// enfileirar todo mundo atrás do lock dela.
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(7262021);
		SET LOCAL lock_timeout = '10s';
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var current int
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("migrate: versão atual: %w", err)
	}
	for i, f := range migrationFiles() {
		version := i + 1 // TestMigrationFilesAreNumbered garante q o nome bate com a posição
		if version <= current {
			continue
		}
		sql, _ := migrations.ReadFile(f) // arquivo embutido, listado agora
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migrate: %s: %w", f, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			return fmt.Errorf("migrate: registrar %s: %w", f, err)
		}
	}
	return tx.Commit(ctx)
}

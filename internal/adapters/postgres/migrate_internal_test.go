package postgres

import (
	"fmt"
	"strings"
	"testing"
)

// A versão gravada é a posição do arquivo: o nome tem q começar com ela, sem buraco.
func TestMigrationFilesAreNumbered(t *testing.T) {
	files := migrationFiles()
	if len(files) == 0 {
		t.Fatal("nenhuma migração embutida")
	}
	for i, f := range files {
		if want := fmt.Sprintf("migrations/%03d_", i+1); !strings.HasPrefix(f, want) {
			t.Errorf("%s: esperado prefixo %s", f, want)
		}
	}
}
